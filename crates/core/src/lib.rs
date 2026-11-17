//! Pure Capsule Nexus state transitions.
//!
//! The proof-of-concept Memory is deliberately process-lifetime and
//! nondurable. External execution and textualization live outside this crate.
//!
//! The Core also holds what a session's subscribers see: one
//! [`nexus::Publisher`] per session that has a subscriber, its state the
//! session's panes that are not Dead. A change is the pane whole, its
//! [`Known`]: it is published by the operation that made it, once Memory
//! answers that it succeeded, and only when the Known moved. A Configure of
//! the subscriber capacity resizes every session's Publisher.

pub mod generated;

pub use generated::operation::Operation;

use emission_capsule_nexus::PaneObservation;
use generated::memory::{Answer, OwnedPane, PendingCreation};
use nexus::{Capacity, Publisher, Publishing, Reached};
use signal_capsule_nexus::{
    Attempt, Command, CreationId, Keys, Known, Label, MemoryRefusal, ObservedEffect,
    Outcome, Pane, Panes, Refusal, Session, State, StorePhase, StoreRefusal, Text,
};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalResult<T> {
    Succeeded(T),
    Refused(Refusal),
    Unavailable(signal_capsule_nexus::TransportRefusal),
    Uncertain,
}

pub trait Memory {
    fn owned(&self, pane: &Pane) -> Result<Option<OwnedPane>, MemoryRefusal>;
    fn owned_in_session(&self, session: &Session) -> Result<Vec<OwnedPane>, MemoryRefusal>;
    fn project_listing(
        &self,
        session: &Session,
        observed: &[PaneObservation],
    ) -> Result<Vec<Known>, MemoryRefusal>;
    fn store_owned(&mut self, pane: OwnedPane) -> Answer;
    fn close_owned(&mut self, pane: &Pane) -> Answer;
    fn store_attempt(&mut self, pane: &Pane, attempt: Attempt) -> Answer;
    fn reconcile_listing(
        &mut self,
        session: &Session,
        observed: &[PaneObservation],
    ) -> Answer;
    fn pending(&self, creation_id: &CreationId) -> Result<Option<PendingCreation>, MemoryRefusal>;
    fn pending_in_session(&self, session: &Session) -> Result<Vec<PendingCreation>, MemoryRefusal>;
    fn store_pending(&mut self, pending: PendingCreation) -> Answer;
    fn finish_created(&mut self, creation_id: &CreationId, pane: Pane) -> Answer;
    fn discard_pending(&mut self, creation_id: &CreationId) -> Answer;
}

#[derive(Default)]
pub struct InMemory {
    panes: HashMap<Pane, OwnedPane>,
    pending: HashMap<CreationId, PendingCreation>,
}

impl InMemory {
    fn project_listing(&self, session: &Session, observed: &[PaneObservation]) -> Vec<Known> {
        let by_pane: HashMap<&Pane, &PaneObservation> =
            observed.iter().map(|seen| (&seen.pane, seen)).collect();
        self.panes
            .values()
            .filter(|owned| &owned.pane.session == session)
            .map(|owned| {
                Known {
                    pane: owned.pane.clone(),
                    label_option: owned.label_option.clone(),
                    state: reconciled_state(owned, by_pane.get(&owned.pane).copied()),
                    attempt: owned.attempt.clone(),
                }
            })
            .collect()
    }
}

impl Memory for InMemory {
    fn owned(&self, pane: &Pane) -> Result<Option<OwnedPane>, MemoryRefusal> {
        Ok(self.panes.get(pane).cloned())
    }

    fn owned_in_session(&self, session: &Session) -> Result<Vec<OwnedPane>, MemoryRefusal> {
        Ok(self
            .panes
            .values()
            .filter(|owned| &owned.pane.session == session)
            .cloned()
            .collect())
    }

    fn project_listing(
        &self,
        session: &Session,
        observed: &[PaneObservation],
    ) -> Result<Vec<Known>, MemoryRefusal> {
        Ok(self.project_listing(session, observed))
    }

    fn store_owned(&mut self, pane: OwnedPane) -> Answer {
        self.panes.insert(pane.pane.clone(), pane);
        Answer::Succeeded
    }

    fn close_owned(&mut self, pane: &Pane) -> Answer {
        self.panes.remove(pane);
        Answer::Succeeded
    }

    fn store_attempt(&mut self, pane: &Pane, attempt: Attempt) -> Answer {
        if let Some(owned) = self.panes.get_mut(pane) {
            owned.attempt = attempt;
            Answer::Succeeded
        } else {
            Answer::Failed(MemoryRefusal(signal_capsule_nexus::Message(
                "attempt target is not owned".into(),
            )))
        }
    }

    fn reconcile_listing(
        &mut self,
        session: &Session,
        observed: &[PaneObservation],
    ) -> Answer {
        let by_pane: HashMap<&Pane, &PaneObservation> =
            observed.iter().map(|seen| (&seen.pane, seen)).collect();
        for owned in self
            .panes
            .values_mut()
            .filter(|owned| &owned.pane.session == session)
        {
            owned.state = reconciled_state(owned, by_pane.get(&owned.pane).copied());
            owned.attempt = Attempt::Settled;
        }
        Answer::Succeeded
    }

    fn pending(&self, creation_id: &CreationId) -> Result<Option<PendingCreation>, MemoryRefusal> {
        Ok(self.pending.get(creation_id).cloned())
    }

    fn pending_in_session(&self, session: &Session) -> Result<Vec<PendingCreation>, MemoryRefusal> {
        Ok(self
            .pending
            .values()
            .filter(|pending| &pending.workspace.session == session)
            .cloned()
            .collect())
    }

    fn store_pending(&mut self, pending: PendingCreation) -> Answer {
        if self.pending.contains_key(&pending.creation_id) {
            return Answer::Failed(MemoryRefusal(signal_capsule_nexus::Message(
                "creation identifier is already pending".into(),
            )));
        }
        self.pending.insert(pending.creation_id.clone(), pending);
        Answer::Succeeded
    }

    fn finish_created(&mut self, creation_id: &CreationId, pane: Pane) -> Answer {
        let Some(pending) = self.pending.get(creation_id).cloned() else {
            return Answer::Failed(MemoryRefusal(signal_capsule_nexus::Message(
                "creation identifier is not pending".into(),
            )));
        };
        if self.panes.contains_key(&pane) {
            return Answer::Failed(MemoryRefusal(signal_capsule_nexus::Message(
                "created pane is already owned".into(),
            )));
        }
        self.pending.remove(creation_id);
        self.panes.insert(
            pane.clone(),
            OwnedPane {
                pane,
                workspace: pending.workspace,
                label_option: None,
                state: State::Open,
                attempt: Attempt::Settled,
            },
        );
        Answer::Succeeded
    }

    fn discard_pending(&mut self, creation_id: &CreationId) -> Answer {
        if self.pending.remove(creation_id).is_some() {
            Answer::Succeeded
        } else {
            Answer::Failed(MemoryRefusal(signal_capsule_nexus::Message(
                "creation identifier is not pending".into(),
            )))
        }
    }
}

fn reconciled_state(owned: &OwnedPane, seen: Option<&PaneObservation>) -> State {
    match seen {
        None => State::Dead,
        Some(seen)
            if seen.state == State::Open
                && matches!(owned.state, State::Busy | State::Idle | State::Dead) =>
        {
            State::Dead
        }
        Some(seen) => seen.state.clone(),
    }
}

/// An owned pane as a subscriber sees it.
impl From<&OwnedPane> for Known {
    fn from(owned: &OwnedPane) -> Self {
        Known {
            pane: owned.pane.clone(),
            label_option: owned.label_option.clone(),
            state: owned.state.clone(),
            attempt: owned.attempt.clone(),
        }
    }
}

pub struct Core<M> {
    memory: M,
    publications: HashMap<Session, Publication>,
}

/// A session's subscription: its Publisher, the streams open on it, and the
/// end of the feed that observes the session for it, sent when the last
/// stream is dropped.
struct Publication {
    publisher: Publisher<Panes, Known>,
    streams: usize,
    feed: tokio::sync::oneshot::Sender<()>,
}

/// One stream opened on a session's Publisher. A stream that made the
/// Publisher carries the feed's end: it resolves when the publication is
/// dropped with its last stream.
pub struct Subscription {
    pub stream: signal::Stream<Panes, Known>,
    pub feed: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl<M> Core<M> {
    pub fn new(memory: M) -> Self {
        Self {
            memory,
            publications: HashMap::new(),
        }
    }

    pub fn memory(&self) -> &M {
        &self.memory
    }

    /// One stream of the session ended; the last takes the Publisher, and the
    /// feed's end, with it.
    pub fn unsubscribe(&mut self, session: &Session) {
        let Some(publication) = self.publications.get_mut(session) else {
            return;
        };
        publication.streams = publication.streams.saturating_sub(1);
        if publication.streams > 0 {
            return;
        }
        if let Some(Publication { feed, .. }) = self.publications.remove(session) {
            let _ = feed.send(());
        }
    }

    /// How many sessions have a Publisher.
    pub fn publication_count(&self) -> usize {
        self.publications.len()
    }

    /// Moves every session's Publisher to the capacity; each open stream
    /// follows it. Yields the streams moved, across every session.
    pub fn resize(&mut self, capacity: Capacity) -> Reached {
        Reached(
            self.publications
                .values_mut()
                .map(|publication| {
                    publication
                        .publisher
                        .resize(capacity)
                        .expect("the configured subscriber capacity is validated positive")
                        .0
                })
                .sum(),
        )
    }

    /// Hands the pane whole to the session's subscribers when its Known
    /// moved: a pane is told Dead once, and a pane first seen Dead is not told.
    fn publish(&self, before: Option<&Known>, after: Known) {
        let moved = match before {
            Some(before) => *before != after,
            None => after.state != State::Dead,
        };
        if !moved {
            return;
        }
        if let Some(publication) = self.publications.get(&after.pane.session) {
            publication.publisher.publish(after);
        }
    }
}

impl<M: Memory> Core<M> {
    pub fn prepare_creation(&mut self, pending: PendingCreation) -> Result<(), Outcome> {
        match self.memory.store_pending(pending) {
            Answer::Succeeded => Ok(()),
            Answer::Failed(refusal) => Err(store_refused(refusal, StorePhase::BeforeEmission)),
        }
    }

    pub fn create(&mut self, creation_id: CreationId, external: ExternalResult<Pane>) -> Outcome {
        match external {
            ExternalResult::Succeeded(pane) => {
                match self.memory.finish_created(&creation_id, pane.clone()) {
                    Answer::Succeeded => {
                        self.publish(
                            None,
                            Known {
                                pane: pane.clone(),
                                label_option: None,
                                state: State::Open,
                                attempt: Attempt::Settled,
                            },
                        );
                        Outcome::Created(pane)
                    }
                    Answer::Failed(refusal) => {
                        store_refused(refusal, StorePhase::Observed(ObservedEffect::Created(pane)))
                    }
                }
            }
            ExternalResult::Refused(refusal) => match self.memory.discard_pending(&creation_id) {
                Answer::Succeeded => Outcome::Refused(refusal),
                Answer::Failed(memory_refusal) => store_refused(memory_refusal, StorePhase::Uncertain),
            },
            ExternalResult::Unavailable(refusal) => match self.memory.discard_pending(&creation_id) {
                Answer::Succeeded => Outcome::Unavailable(refusal),
                Answer::Failed(memory_refusal) => store_refused(memory_refusal, StorePhase::Uncertain),
            },
            ExternalResult::Uncertain => Outcome::Uncertain,
        }
    }

    pub fn pending_in_session(&self, session: &Session) -> Result<Vec<PendingCreation>, Outcome> {
        self.memory
            .pending_in_session(session)
            .map_err(|refusal| store_refused(refusal, StorePhase::BeforeEmission))
    }

    pub fn start(
        &mut self,
        pane: Pane,
        _command: Command,
        label: Label,
        external: ExternalResult<State>,
    ) -> Outcome {
        let mut owned = match self.require_owned(&pane) {
            Ok(owned) => owned,
            Err(outcome) => return outcome,
        };
        match external {
            ExternalResult::Succeeded(state) => {
                let before = Known::from(&owned);
                owned.label_option = Some(label);
                owned.state = state;
                owned.attempt = Attempt::Settled;
                let after = Known::from(&owned);
                match self.memory.store_owned(owned) {
                    Answer::Succeeded => {
                        self.publish(Some(&before), after);
                        Outcome::Done
                    }
                    Answer::Failed(refusal) => {
                        store_refused(refusal, StorePhase::Observed(ObservedEffect::Done))
                    }
                }
            }
            ExternalResult::Refused(refusal) => Outcome::Refused(refusal),
            ExternalResult::Unavailable(refusal) => Outcome::Unavailable(refusal),
            ExternalResult::Uncertain => self.retain_uncertain(&owned),
        }
    }

    pub fn prompt(&mut self, pane: Pane, _text: Text, external: ExternalResult<()>) -> Outcome {
        match self.require_owned(&pane) {
            Ok(owned) => self.finish_done(&owned, external),
            Err(outcome) => outcome,
        }
    }

    pub fn press(&mut self, pane: Pane, _keys: Keys, external: ExternalResult<()>) -> Outcome {
        match self.require_owned(&pane) {
            Ok(owned) => self.finish_done(&owned, external),
            Err(outcome) => outcome,
        }
    }

    pub fn close(&mut self, pane: Pane, external: ExternalResult<()>) -> Outcome {
        let owned = match self.require_owned(&pane) {
            Ok(owned) => owned,
            Err(outcome) => return outcome,
        };
        match external {
            ExternalResult::Succeeded(()) => match self.memory.close_owned(&pane) {
                Answer::Succeeded => {
                    let before = Known::from(&owned);
                    let after = Known {
                        state: State::Dead,
                        ..before.clone()
                    };
                    self.publish(Some(&before), after);
                    Outcome::Done
                }
                Answer::Failed(refusal) => store_refused(refusal, StorePhase::Observed(ObservedEffect::Done)),
            },
            ExternalResult::Refused(refusal) => Outcome::Refused(refusal),
            ExternalResult::Unavailable(refusal) => Outcome::Unavailable(refusal),
            ExternalResult::Uncertain => self.retain_uncertain(&owned),
        }
    }

    pub fn list(
        &mut self,
        session: Session,
        external: ExternalResult<Vec<PaneObservation>>,
    ) -> Outcome {
        match external {
            ExternalResult::Succeeded(observed) => {
                let projected = match self.memory.project_listing(&session, &observed) {
                    Ok(projected) => projected,
                    Err(refusal) => {
                        return store_refused(
                            refusal,
                            StorePhase::Observed(ObservedEffect::Listed(Vec::new())),
                        )
                    }
                };
                let before = match self.memory.owned_in_session(&session) {
                    Ok(owned) => owned
                        .iter()
                        .map(|owned| (owned.pane.clone(), Known::from(owned)))
                        .collect::<HashMap<_, _>>(),
                    Err(refusal) => {
                        return store_refused(
                            refusal,
                            StorePhase::Observed(ObservedEffect::Listed(projected)),
                        )
                    }
                };
                match self.memory.reconcile_listing(&session, &observed) {
                    Answer::Succeeded => {
                        // The listing settles every attempt, as Memory records.
                        for known in &projected {
                            let after = Known {
                                attempt: Attempt::Settled,
                                ..known.clone()
                            };
                            self.publish(before.get(&known.pane), after);
                        }
                        Outcome::Listed(projected)
                    }
                    Answer::Failed(refusal) => store_refused(
                        refusal,
                        StorePhase::Observed(ObservedEffect::Listed(projected)),
                    ),
                }
            }
            ExternalResult::Refused(refusal) => Outcome::Refused(refusal),
            ExternalResult::Unavailable(refusal) => Outcome::Unavailable(refusal),
            ExternalResult::Uncertain => Outcome::Uncertain,
        }
    }

    /// Opens a stream on the session's Publisher, making it at the session's
    /// first stream over the panes Memory holds there that are not Dead,
    /// whoever owns them. Opening changes nothing in Memory.
    pub fn subscribe(&mut self, session: &Session, capacity: Capacity) -> Result<Subscription, Outcome> {
        if let Some(publication) = self.publications.get_mut(session) {
            publication.streams += 1;
            return Ok(Subscription {
                stream: publication.publisher.open(),
                feed: None,
            });
        }
        let mut living = self
            .memory
            .owned_in_session(session)
            .map_err(|refusal| store_refused(refusal, StorePhase::BeforeEmission))?
            .into_iter()
            .filter(|owned| owned.state != State::Dead)
            .map(|owned| Known::from(&owned))
            .collect::<Vec<_>>();
        living.sort_by(|left, right| left.pane.pane_id.cmp(&right.pane.pane_id));
        let publisher = Publisher::create(Panes(living), capacity)
            .expect("the configured subscriber capacity is validated positive");
        let stream = publisher.open();
        let (feed, ended) = tokio::sync::oneshot::channel();
        self.publications.insert(
            session.clone(),
            Publication {
                publisher,
                streams: 1,
                feed,
            },
        );
        Ok(Subscription {
            stream,
            feed: Some(ended),
        })
    }

    fn require_owned(&self, pane: &Pane) -> Result<OwnedPane, Outcome> {
        match self.memory.owned(pane) {
            Ok(Some(owned)) => Ok(owned),
            Ok(None) => Err(Outcome::Unknown(pane.clone())),
            Err(refusal) => Err(store_refused(refusal, StorePhase::BeforeEmission)),
        }
    }

    /// Checks ownership before an external effect. Callers keep their own
    /// reservation beside this Core while the effect is in flight.
    pub fn preflight_owned(&self, pane: &Pane) -> Result<(), Outcome> {
        self.require_owned(pane).map(|_| ())
    }

    /// Returns whether a session has any owned panes. An empty owned set is a
    /// complete local List result and owes no external observation.
    pub fn preflight_session(&self, session: &Session) -> Result<bool, Outcome> {
        match self.memory.owned_in_session(session) {
            Ok(owned) => Ok(!owned.is_empty()),
            Err(refusal) => Err(store_refused(refusal, StorePhase::BeforeEmission)),
        }
    }

    pub fn owned_in_session(&self, session: &Session) -> Result<Vec<OwnedPane>, Outcome> {
        self.memory
            .owned_in_session(session)
            .map_err(|refusal| store_refused(refusal, StorePhase::BeforeEmission))
    }

    fn finish_done(&mut self, owned: &OwnedPane, external: ExternalResult<()>) -> Outcome {
        match external {
            ExternalResult::Succeeded(()) => {
                self.record_attempt(owned, Attempt::Settled, Outcome::Done, StorePhase::Observed(ObservedEffect::Done))
            }
            ExternalResult::Refused(refusal) => Outcome::Refused(refusal),
            ExternalResult::Unavailable(refusal) => Outcome::Unavailable(refusal),
            ExternalResult::Uncertain => self.retain_uncertain(owned),
        }
    }

    fn retain_uncertain(&mut self, owned: &OwnedPane) -> Outcome {
        self.record_attempt(owned, Attempt::Uncertain, Outcome::Uncertain, StorePhase::Uncertain)
    }

    /// Records the pane's attempt and publishes the pane when it moved; the
    /// outcome when Memory records it, else the refusal in its phase.
    fn record_attempt(
        &mut self,
        owned: &OwnedPane,
        attempt: Attempt,
        outcome: Outcome,
        phase: StorePhase,
    ) -> Outcome {
        match self.memory.store_attempt(&owned.pane, attempt.clone()) {
            Answer::Succeeded => {
                let before = Known::from(owned);
                let after = Known {
                    attempt,
                    ..before.clone()
                };
                self.publish(Some(&before), after);
                outcome
            }
            Answer::Failed(refusal) => store_refused(refusal, phase),
        }
    }
}

fn store_refused(memory_refusal: MemoryRefusal, store_phase: StorePhase) -> Outcome {
    Outcome::StoreRefused(StoreRefusal {
        memory_refusal,
        store_phase,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_capsule_nexus::{
        InstanceId, Message, Name, PaneId, Root, Sequence, Value, Variable, Workspace,
    };
    use std::cell::Cell;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Call {
        Owned,
        StoreOwned,
        CloseOwned,
        StoreAttempt,
        ReconcileListing,
        StorePending,
        FinishCreated,
        DiscardPending,
    }

    struct RefusingMemory {
        memory: InMemory,
        next: Cell<Option<Call>>,
    }

    impl RefusingMemory {
        fn new(memory: InMemory, next: Call) -> Self {
            Self {
                memory,
                next: Cell::new(Some(next)),
            }
        }

        fn refusal(&mut self, call: Call) -> Answer {
            if self.next.get() == Some(call) {
                self.next.set(None);
                Answer::Failed(MemoryRefusal(Message("selected test refusal".into())))
            } else {
                Answer::Succeeded
            }
        }
    }

    impl Memory for RefusingMemory {
        fn owned(&self, pane: &Pane) -> Result<Option<OwnedPane>, MemoryRefusal> {
            if self.next.get() == Some(Call::Owned) {
                self.next.set(None);
                Err(MemoryRefusal(Message("selected test refusal".into())))
            } else {
                self.memory.owned(pane)
            }
        }

        fn owned_in_session(&self, session: &Session) -> Result<Vec<OwnedPane>, MemoryRefusal> {
            if self.next.get() == Some(Call::Owned) {
                self.next.set(None);
                Err(MemoryRefusal(Message("selected test refusal".into())))
            } else {
                self.memory.owned_in_session(session)
            }
        }

        fn project_listing(&self, session: &Session, observed: &[PaneObservation]) -> Result<Vec<Known>, MemoryRefusal> {
            Ok(self.memory.project_listing(session, observed))
        }

        fn store_owned(&mut self, pane: OwnedPane) -> Answer {
            match self.refusal(Call::StoreOwned) {
                Answer::Succeeded => self.memory.store_owned(pane),
                failed => failed,
            }
        }

        fn close_owned(&mut self, pane: &Pane) -> Answer {
            match self.refusal(Call::CloseOwned) {
                Answer::Succeeded => self.memory.close_owned(pane),
                failed => failed,
            }
        }

        fn store_attempt(&mut self, pane: &Pane, attempt: Attempt) -> Answer {
            match self.refusal(Call::StoreAttempt) {
                Answer::Succeeded => self.memory.store_attempt(pane, attempt),
                failed => failed,
            }
        }

        fn reconcile_listing(
            &mut self,
            session: &Session,
            observed: &[PaneObservation],
        ) -> Answer {
            if self.next.get() == Some(Call::ReconcileListing) {
                self.next.set(None);
                return Answer::Failed(MemoryRefusal(Message("selected test refusal".into())));
            }
            self.memory.reconcile_listing(session, observed)
        }

        fn pending(&self, creation_id: &CreationId) -> Result<Option<PendingCreation>, MemoryRefusal> {
            self.memory.pending(creation_id)
        }

        fn pending_in_session(&self, session: &Session) -> Result<Vec<PendingCreation>, MemoryRefusal> {
            self.memory.pending_in_session(session)
        }

        fn store_pending(&mut self, pending: PendingCreation) -> Answer {
            match self.refusal(Call::StorePending) {
                Answer::Succeeded => self.memory.store_pending(pending),
                failed => failed,
            }
        }

        fn finish_created(&mut self, creation_id: &CreationId, pane: Pane) -> Answer {
            match self.refusal(Call::FinishCreated) {
                Answer::Succeeded => self.memory.finish_created(creation_id, pane),
                failed => failed,
            }
        }

        fn discard_pending(&mut self, creation_id: &CreationId) -> Answer {
            match self.refusal(Call::DiscardPending) {
                Answer::Succeeded => self.memory.discard_pending(creation_id),
                failed => failed,
            }
        }
    }

    fn pane() -> Pane {
        Pane {
            session: Session("v1".into()),
            pane_id: PaneId("p1".into()),
        }
    }

    fn workspace() -> Workspace {
        Workspace {
            session: Session("v1".into()),
            root: Root("/tmp/v1".into()),
            variable_vector: vec![Variable {
                name: Name("FLOW_ID".into()),
                value: Value("af8fe3".into()),
            }],
        }
    }

    fn creation_id() -> CreationId {
        CreationId {
            instance_id: InstanceId("instance".into()),
            sequence: Sequence(1),
        }
    }

    fn pending() -> PendingCreation {
        PendingCreation {
            creation_id: creation_id(),
            principal: translator::Principal::Login(translator::UserId(1000)),
            workspace: workspace(),
        }
    }

    fn seeded() -> InMemory {
        let mut memory = InMemory::default();
        memory
            .store_owned(OwnedPane {
                pane: pane(),
                workspace: workspace(),
                label_option: Some(Label("mind/versionOne".into())),
                state: State::Open,
                attempt: Attempt::Settled,
            })
            ;
        memory
    }

    #[test]
    fn creation_success_is_owned() {
        let mut core = Core::new(InMemory::default());
        core.prepare_creation(pending()).expect("pending creation");
        assert_eq!(
            core.create(creation_id(), ExternalResult::Succeeded(pane())),
            Outcome::Created(pane())
        );
        assert!(core.memory().owned(&pane()).expect("read").is_some());
    }

    #[test]
    fn creation_store_refusal_precedes_any_external_attempt() {
        let mut core = Core::new(RefusingMemory::new(
            InMemory::default(),
            Call::StorePending,
        ));
        assert!(matches!(
            core.prepare_creation(pending()),
            Err(Outcome::StoreRefused(StoreRefusal {
                store_phase: StorePhase::BeforeEmission,
                ..
            }))
        ));
    }

    #[test]
    fn uncertain_creation_keeps_one_pending_fact_without_a_pane() {
        let mut core = Core::new(InMemory::default());
        core.prepare_creation(pending()).expect("pending creation");
        assert_eq!(
            core.create(creation_id(), ExternalResult::Uncertain),
            Outcome::Uncertain
        );
        assert!(core.memory().owned(&pane()).expect("owned read").is_none());
        assert_eq!(
            core.memory().pending(&creation_id()).expect("pending read"),
            Some(pending())
        );
    }

    #[test]
    fn unknown_pane_stops_before_external_result_matters() {
        let mut core = Core::new(InMemory::default());
        assert_eq!(
            core.prompt(pane(), Text("hello".into()), ExternalResult::Succeeded(())),
            Outcome::Unknown(pane())
        );
    }

    #[test]
    fn memory_refuses_to_claim_an_attempt_change_for_an_absent_pane() {
        let mut memory = InMemory::default();
        assert!(matches!(
            memory.store_attempt(&pane(), Attempt::Uncertain),
            Answer::Failed(_)
        ));
    }

    #[test]
    fn external_refusal_is_preserved() {
        let refusal = Refusal {
            error_code: signal_capsule_nexus::ErrorCode("agent_not_ready".into()),
            exit_status_option: None,
            message: Message("agent is busy".into()),
        };
        let mut core = Core::new(seeded());
        assert_eq!(
            core.prompt(pane(), Text("hello".into()), ExternalResult::Refused(refusal.clone())),
            Outcome::Refused(refusal)
        );
    }

    #[test]
    fn uncertainty_is_bounded_by_the_owned_pane() {
        let mut core = Core::new(seeded());
        assert_eq!(
            core.prompt(pane(), Text("hello".into()), ExternalResult::Uncertain),
            Outcome::Uncertain
        );
        assert_eq!(
            core.memory().owned(&pane()).expect("read").expect("owned").attempt,
            Attempt::Uncertain
        );
    }

    #[test]
    fn repeated_uncertainty_remains_one_owned_pane_fact() {
        let mut core = Core::new(seeded());
        assert_eq!(
            core.prompt(pane(), Text("hello".into()), ExternalResult::Uncertain),
            Outcome::Uncertain
        );
        assert_eq!(
            core.press(pane(), Keys(vec!["x".into()]), ExternalResult::Uncertain),
            Outcome::Uncertain
        );
        let owned = core.memory().owned(&pane()).expect("read").expect("owned");
        assert_eq!(owned.attempt, Attempt::Uncertain);
    }

    #[test]
    fn observed_success_and_later_store_refusal_are_both_returned() {
        let mut memory = InMemory::default();
        memory.store_pending(pending());
        let mut core = Core::new(RefusingMemory::new(memory, Call::FinishCreated));
        let outcome = core.create(creation_id(), ExternalResult::Succeeded(pane()));
        assert!(matches!(
            outcome,
            Outcome::StoreRefused(StoreRefusal {
                store_phase: StorePhase::Observed(ObservedEffect::Created(_)),
                ..
            })
        ));
    }

    #[test]
    fn uncertainty_store_refusal_is_distinct() {
        let mut core = Core::new(RefusingMemory::new(seeded(), Call::StoreAttempt));
        let outcome = core.press(pane(), Keys(vec!["x".into()]), ExternalResult::Uncertain);
        assert!(matches!(
            outcome,
            Outcome::StoreRefused(StoreRefusal {
                store_phase: StorePhase::Uncertain,
                ..
            })
        ));
    }

    #[test]
    fn absence_becomes_dead_only_on_complete_successful_listing() {
        let mut core = Core::new(seeded());
        let outcome = core.list(Session("v1".into()), ExternalResult::Succeeded(vec![]));
        assert_eq!(
            outcome,
            Outcome::Listed(vec![Known {
                pane: pane(),
                label_option: Some(Label("mind/versionOne".into())),
                state: State::Dead,
                attempt: Attempt::Settled,
            }])
        );
    }

    fn observed(state: State) -> ExternalResult<Vec<PaneObservation>> {
        ExternalResult::Succeeded(vec![PaneObservation { pane: pane(), state }])
    }

    fn known_of(state: State) -> Known {
        Known {
            pane: pane(),
            label_option: Some(Label("mind/versionOne".into())),
            state,
            attempt: Attempt::Settled,
        }
    }

    fn frame_of(state: State) -> signal::Frame<Panes, Known> {
        signal::Frame::Changed(known_of(state))
    }

    fn command() -> Command {
        use signal_capsule_nexus::{AgentKind, AgentName, Executable};
        Command {
            agent_name: AgentName("probe".into()),
            agent_kind: AgentKind::Codex,
            executable: Executable::Configured,
            string_vector: Vec::new(),
        }
    }

    fn panes_of(state: State) -> Panes {
        Panes(vec![Known {
            pane: pane(),
            label_option: Some(Label("mind/versionOne".into())),
            state,
            attempt: Attempt::Settled,
        }])
    }

    #[tokio::test]
    async fn a_subscription_opens_on_the_panes_that_are_not_dead() {
        use futures_util::StreamExt;
        let mut memory = seeded();
        let dead = Pane {
            session: pane().session,
            pane_id: PaneId("gone".into()),
        };
        memory.store_owned(OwnedPane {
            pane: dead,
            workspace: workspace(),
            label_option: None,
            state: State::Dead,
            attempt: Attempt::Settled,
        });
        let mut core = Core::new(memory);
        let Subscription { mut stream, feed } = core
            .subscribe(&pane().session, Capacity(8))
            .expect("Memory gives the panes");
        assert!(feed.is_some(), "the first stream starts the feed");
        assert_eq!(stream.next().await, Some(signal::Frame::Opened(panes_of(State::Open))));
    }

    #[tokio::test]
    async fn each_change_is_published_once_after_memory_succeeds() {
        use futures_util::StreamExt;
        let mut core = Core::new(seeded());
        let Subscription { mut stream, .. } = core
            .subscribe(&pane().session, Capacity(8))
            .expect("Memory gives the panes");
        assert!(matches!(core.list(pane().session, observed(State::Busy)), Outcome::Listed(_)));
        assert!(matches!(core.list(pane().session, observed(State::Busy)), Outcome::Listed(_)));
        assert!(matches!(core.list(pane().session, observed(State::Idle)), Outcome::Listed(_)));
        assert_eq!(core.close(pane(), ExternalResult::Succeeded(())), Outcome::Done);
        assert_eq!(stream.next().await, Some(signal::Frame::Opened(panes_of(State::Open))));
        assert_eq!(stream.next().await, Some(frame_of(State::Busy)));
        assert_eq!(stream.next().await, Some(frame_of(State::Idle)));
        assert_eq!(stream.next().await, Some(frame_of(State::Dead)));
        let late = core
            .subscribe(&pane().session, Capacity(8))
            .expect("a second stream opens on the Publisher");
        assert!(late.feed.is_none(), "one Publisher and one feed per session");
        let mut late = late.stream;
        assert_eq!(late.next().await, Some(signal::Frame::Opened(Panes(Vec::new()))));
    }

    #[tokio::test]
    async fn a_lagged_subscriber_is_given_the_state_and_goes_on() {
        use futures_util::StreamExt;
        let mut core = Core::new(seeded());
        let Subscription { mut stream, .. } = core
            .subscribe(&pane().session, Capacity(1))
            .expect("Memory gives the panes");
        for state in [State::Busy, State::Idle, State::Busy] {
            assert!(matches!(core.list(pane().session, observed(state)), Outcome::Listed(_)));
        }
        assert_eq!(stream.next().await, Some(signal::Frame::Opened(panes_of(State::Open))));
        assert_eq!(stream.next().await, Some(signal::Frame::Lagged(panes_of(State::Busy))));
        assert!(matches!(core.list(pane().session, observed(State::Idle)), Outcome::Listed(_)));
        assert_eq!(stream.next().await, Some(frame_of(State::Idle)));
    }

    #[tokio::test]
    async fn a_refused_memory_change_publishes_nothing() {
        use futures_util::StreamExt;
        let mut core = Core::new(RefusingMemory::new(seeded(), Call::CloseOwned));
        let Subscription { mut stream, .. } = core
            .subscribe(&pane().session, Capacity(8))
            .expect("Memory gives the panes");
        assert!(matches!(core.close(pane(), ExternalResult::Succeeded(())), Outcome::StoreRefused(_)));
        assert!(matches!(core.list(pane().session, observed(State::Idle)), Outcome::Listed(_)));
        assert_eq!(stream.next().await, Some(signal::Frame::Opened(panes_of(State::Open))));
        assert_eq!(stream.next().await, Some(frame_of(State::Idle)));
    }

    #[tokio::test]
    async fn a_start_publishes_the_pane_whole_with_its_label() {
        use futures_util::StreamExt;
        let mut core = Core::new(seeded());
        let Subscription { mut stream, .. } = core
            .subscribe(&pane().session, Capacity(8))
            .expect("Memory gives the panes");
        let started = core.start(
            pane(),
            command(),
            Label("mind/designer".into()),
            ExternalResult::Succeeded(State::Open),
        );
        assert_eq!(started, Outcome::Done);
        assert_eq!(stream.next().await, Some(signal::Frame::Opened(panes_of(State::Open))));
        assert_eq!(
            stream.next().await,
            Some(signal::Frame::Changed(Known {
                label_option: Some(Label("mind/designer".into())),
                ..known_of(State::Open)
            }))
        );
    }

    #[tokio::test]
    async fn an_attempt_that_moved_is_published_and_settled_by_the_listing() {
        use futures_util::StreamExt;
        let mut core = Core::new(seeded());
        let Subscription { mut stream, .. } = core
            .subscribe(&pane().session, Capacity(8))
            .expect("Memory gives the panes");
        assert_eq!(core.prompt(pane(), Text("hello".into()), ExternalResult::Uncertain), Outcome::Uncertain);
        assert_eq!(core.prompt(pane(), Text("hello".into()), ExternalResult::Uncertain), Outcome::Uncertain);
        assert!(matches!(core.list(pane().session, observed(State::Open)), Outcome::Listed(_)));
        assert_eq!(stream.next().await, Some(signal::Frame::Opened(panes_of(State::Open))));
        assert_eq!(
            stream.next().await,
            Some(signal::Frame::Changed(Known {
                attempt: Attempt::Uncertain,
                ..known_of(State::Open)
            }))
        );
        assert_eq!(stream.next().await, Some(frame_of(State::Open)));
    }

    #[tokio::test]
    async fn a_resize_moves_every_open_stream_of_every_session() {
        use futures_util::StreamExt;
        let mut core = Core::new(seeded());
        let Subscription { mut stream, .. } = core
            .subscribe(&pane().session, Capacity(8))
            .expect("Memory gives the panes");
        let _second = core
            .subscribe(&pane().session, Capacity(8))
            .expect("a second stream opens on the Publisher");
        let other = Session("other".into());
        let _third = core.subscribe(&other, Capacity(8)).expect("an empty session subscribes");
        for state in [State::Busy, State::Idle] {
            assert!(matches!(core.list(pane().session, observed(state)), Outcome::Listed(_)));
        }
        assert_eq!(core.resize(Capacity(1)), Reached(3));
        assert_eq!(stream.next().await, Some(signal::Frame::Opened(panes_of(State::Open))));
        assert_eq!(stream.next().await, Some(signal::Frame::Lagged(panes_of(State::Idle))));
        assert!(matches!(core.list(pane().session, observed(State::Busy)), Outcome::Listed(_)));
        assert_eq!(stream.next().await, Some(frame_of(State::Busy)));
    }

    #[test]
    fn the_last_stream_takes_the_publisher_and_ends_the_feed() {
        let mut core = Core::new(seeded());
        let first = core.subscribe(&pane().session, Capacity(8)).expect("first");
        let second = core.subscribe(&pane().session, Capacity(8)).expect("second");
        let mut feed = first.feed.expect("the first stream starts the feed");
        assert!(second.feed.is_none());
        assert_eq!(core.publication_count(), 1);
        core.unsubscribe(&pane().session);
        assert_eq!(core.publication_count(), 1);
        assert_eq!(feed.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty));
        core.unsubscribe(&pane().session);
        assert_eq!(core.publication_count(), 0);
        assert_eq!(feed.try_recv(), Ok(()), "the last stream ends the feed");
    }
}
