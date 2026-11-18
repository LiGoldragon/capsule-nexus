//! The Capsule Nexus daemon: its configuration lifecycle, its actor and the
//! bodies its sockets are served with. The serving itself is every Nexus's,
//! in `nexus-serving`.

use capsule_nexus_wire::CapsuleNexus;
use meta_signal_capsule_nexus::{BootstrapRefusal, Message, SocketPath};
use nexus_serving::{
    Closable, Closing, EmissionFailure, EmissionLane, Emitting, Listeners, Listening,
    MetaAnswering, PeerAdmitting, PublicServing, Reply, SubscriptionClosing, TransportBounded,
};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[cfg(test)]
use nexus_serving::ConnectionServing;

/// The Capsule Nexus's four socket paths in its place.
pub type SocketPaths = nexus_serving::SocketPaths<CapsuleNexus>;

/// One accepted public connection, with the authority it was admitted under.
pub type AcceptedPublic = nexus_serving::AcceptedPublic<PublicAuthority>;

/// One public connection, served with the Dispatcher's bodies.
pub type PublicConnection = nexus_serving::PublicConnection<Dispatcher>;

pub struct Bootstrap {
    listeners: Listeners<CapsuleNexus>,
    store: Arc<ConfigurationStore>,
}

impl Bootstrap {
    pub fn bind(
        place: &shared_translator::placement::Place,
        admission: capsule_nexus_wire::ValidAdmission,
        translator_peer: shared_translator::PeerIdentity,
    ) -> std::io::Result<Self> {
        Ok(Self {
            listeners: Listeners::bind(place, admission, translator_peer)?,
            store: Arc::new(ConfigurationStore::default()),
        })
    }

    pub fn configure(
        &self,
        setting: core_meta_capsule_nexus::CoreSet,
    ) -> core_meta_capsule_nexus::Output {
        let mut staged = None;
        let response = self.store.configure(setting, || {
            let listener = self.listeners.stage_public().map_err(|error| BootstrapRefusal {
                socket_path: SocketPath(self.listeners.paths().public.display().to_string()),
                message: Message(error.to_string()),
            })?;
            staged = Some(listener);
            Ok(())
        });
        if let Some(listener) = staged {
            self.listeners.publish_public(listener);
        }
        response
    }

    pub fn snapshot(&self) -> Option<Arc<ReadyCoreConfiguration>> {
        self.store.snapshot()
    }

    pub fn configuration_store(&self) -> Arc<ConfigurationStore> {
        Arc::clone(&self.store)
    }

    pub fn serve_next_meta(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.listeners.serve_next_meta(self)
    }

    pub fn accept_public(&self) -> std::io::Result<AcceptedPublic> {
        self.listeners.accept_public(&*self.store)
    }

    pub fn paths(&self) -> &SocketPaths {
        self.listeners.paths()
    }

    pub fn admission(&self) -> capsule_nexus_wire::ValidAdmission {
        self.listeners.admission()
    }
}

/// The meta socket answers each Call with what configuring its setting
/// answers.
impl MetaAnswering for Bootstrap {
    type Query = capsule_nexus_wire::meta_call::Input;
    type Response = capsule_nexus_wire::meta_call::Output;

    fn answer(&self, input: Self::Query) -> Self::Response {
        let capsule_nexus_wire::meta_call::Input::Call(call) = input;
        let response = match call.meta_input.0 {
            core_meta_capsule_nexus::Input::Configure(setting) => self.configure(setting),
        };
        capsule_nexus_wire::meta_call::Output::Answered(capsule_nexus_wire::meta_payload::Output(
            response,
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublicAuthority {
    Translator,
    Peer { peer_id: shared_translator::PeerId, generation: u64 },
}

impl PublicAuthority {
    pub fn peer_grant(&self) -> Option<(shared_translator::PeerId, u64)> {
        match self {
            Self::Translator => None,
            Self::Peer { peer_id, generation } => Some((peer_id.clone(), *generation)),
        }
    }
}

/// The public socket admits, besides the paired translator, each peer bound
/// by a meta Configure, under the generation it was bound with.
impl PeerAdmitting for ConfigurationStore {
    type Authority = PublicAuthority;

    fn translator() -> PublicAuthority {
        PublicAuthority::Translator
    }

    fn peer(&self, caller: &shared_translator::Caller) -> Option<PublicAuthority> {
        self.peer_for(caller)
            .map(|(peer_id, generation)| PublicAuthority::Peer { peer_id, generation })
    }
}

pub fn principal_for(
    authority: &PublicAuthority,
    actual: &shared_translator::Caller,
    serialized: &shared_translator::Caller,
) -> std::io::Result<shared_translator::Principal> {
    match authority {
        PublicAuthority::Translator => Ok(shared_translator::Principal::Login(
            serialized.user_id,
        )),
        PublicAuthority::Peer { peer_id, .. }
            if serialized.user_id == actual.user_id
                && serialized.process_id == actual.process_id =>
        {
            Ok(shared_translator::Principal::Peer(peer_id.clone()))
        }
        PublicAuthority::Peer { .. } => Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "configured peer serialized a different Caller",
        )),
    }
}

/// Who an exchange is answered for: its principal, and the peer grant it was
/// admitted under.
pub type PublicGrant = (shared_translator::Principal, Option<(shared_translator::PeerId, u64)>);

/// The public socket's bodies: every exchange is a Call carrying its Caller,
/// admitted under the connection's authority, attempted on the state the
/// [`Capsule`] actor owns, and answered once or, for a Subscribe, with the
/// stream of the session's panes.
impl PublicServing for Dispatcher {
    type Component = CapsuleNexus;
    type Query = capsule_nexus_wire::public_call::Input;
    type Response = capsule_nexus_wire::public_call::Output;
    type Output = signal_capsule_nexus::Output;
    type State = signal_capsule_nexus::Panes;
    type Change = signal_capsule_nexus::Known;
    type Configuration = ReadyCoreConfiguration;
    type Authority = PublicAuthority;
    type Grant = PublicGrant;
    type Input = signal_capsule_nexus::Input;

    fn configuration(&self) -> Option<Arc<ReadyCoreConfiguration>> {
        self.configuration.snapshot()
    }

    /// A configured peer's binding is checked again for every exchange, and
    /// a Caller that is not the connection's own peer ends the connection
    /// unanswered.
    fn admitted(
        &self,
        authority: &PublicAuthority,
        caller: &shared_translator::Caller,
        query: Self::Query,
    ) -> Option<(PublicGrant, signal_capsule_nexus::Input)> {
        let capsule_nexus_wire::public_call::Input::Call(call) = query;
        let peer_grant = authority.peer_grant();
        if let Some((peer_id, generation)) = &peer_grant {
            if !self.peer_grant_is_current(caller, peer_id, *generation) {
                eprintln!("Capsule Nexus public request failed: configured peer binding changed before request dispatch");
                return None;
            }
        }
        let principal = match principal_for(authority, caller, &call.caller) {
            Ok(principal) => principal,
            Err(error) => {
                eprintln!("Capsule Nexus public request failed: {error}");
                return None;
            }
        };
        Some(((principal, peer_grant), call.public_input.0))
    }

    fn answer(
        &self,
        grant: PublicGrant,
        input: signal_capsule_nexus::Input,
        configuration: Arc<ReadyCoreConfiguration>,
    ) -> Reply<signal_capsule_nexus::Output, signal_capsule_nexus::Panes, signal_capsule_nexus::Known>
    {
        match self.dispatch_as(grant.0, input, configuration) {
            signal_capsule_nexus::Answer::Once(response) => Reply::Once(response),
            signal_capsule_nexus::Answer::Subscribed(stream) => Reply::Subscribed(stream),
        }
    }

    /// A configured peer's subscription is withdrawn with its binding.
    fn carried(&self, grant: &PublicGrant, closing: &Arc<SubscriptionClosing>) -> bool {
        let (principal, peer_grant) = grant;
        if let shared_translator::Principal::Peer(peer_id) = principal {
            let registered = peer_grant.as_ref().is_some_and(|(granted_id, generation)| {
                granted_id == peer_id
                    && self.configuration.register_peer_subscriber(peer_id, *generation, closing)
            });
            if !registered {
                eprintln!("Capsule Nexus subscription failed: configured peer binding changed before subscription registration");
                return false;
            }
        }
        true
    }

    fn answered(output: signal_capsule_nexus::Output) -> capsule_nexus_wire::public_call::Output {
        capsule_nexus_wire::public_call::Output::Answered(
            capsule_nexus_wire::public_payload::Output(output),
        )
    }

    fn subscribed(
        frame: signal::Frame<signal_capsule_nexus::Panes, signal_capsule_nexus::Known>,
    ) -> signal_capsule_nexus::Output {
        signal_capsule_nexus::Output::Subscribed(frame)
    }
}

impl TransportBounded for ReadyCoreConfiguration {
    fn transport_timeout(&self) -> Duration {
        self.transport_timeout
    }

    fn frame_capacity(&self) -> signal::FrameCapacity {
        signal::FrameCapacity::from(self.frame_capacity_bytes)
    }
}

pub fn same_login(caller: &shared_translator::Caller) -> bool {
    caller.user_id.0 == i64::from(nix::unistd::getuid().as_raw())
}

/// The public surface of the Nexus: each input attempted on the state its
/// [`Capsule`] actor owns, every external effect emitted between the actor's
/// steps, so no herdr call holds the state.
#[derive(Clone)]
pub struct Dispatcher {
    capsule: kameo::actor::ActorRef<Capsule>,
    emitter: Arc<dyn ExternalEmitter>,
    configuration: Arc<ConfigurationStore>,
    runtime: tokio::runtime::Handle,
}

pub trait ExternalEmitter: Send + Sync {
    fn emit(
        &self,
        configuration: &ReadyCoreConfiguration,
        herdr: emission_capsule_nexus::Herdr,
    ) -> EmissionResult;
}

pub enum EmissionResult {
    External(emission_capsule_nexus::ExternalOutcome),
    Unavailable(signal_capsule_nexus::TransportRefusal),
}

/// The emitter that sends each herdr call to the translator on the emission
/// lane.
struct SocketEmitter {
    lane: EmissionLane,
}

/// The Kameo actor that owns the Capsule's state: Memory and each session's
/// Publisher in its Core, who owns each pane, the attempts in flight and each
/// pane's revision. Every reading and change of that state is one of its
/// messages, handled one at a time; the Publisher's state is held there once.
#[derive(kameo::Actor)]
pub struct Capsule {
    core: capsule_nexus_core::Core<capsule_nexus_core::InMemory>,
    principals: std::collections::HashMap<signal_capsule_nexus::Pane, shared_translator::Principal>,
    reserved: std::collections::HashMap<signal_capsule_nexus::Pane, u64>,
    revisions: std::collections::HashMap<signal_capsule_nexus::Pane, u64>,
    next_reservation: u64,
    instance_id: signal_capsule_nexus::InstanceId,
    next_creation_sequence: i64,
    /// The subscriber capacity every new Publisher is created at, set by the
    /// Configure that resizes the existing ones; the capacity's one holder.
    subscriber_capacity: Option<nexus::Capacity>,
}

/// What a listing captures before it is emitted: each owned pane's revision,
/// the creations pending in the session, and every principal that owns a pane
/// or a pending creation there.
type Captured = (
    std::collections::HashMap<signal_capsule_nexus::Pane, u64>,
    Vec<capsule_nexus_core::generated::memory::PendingCreation>,
    Vec<shared_translator::Principal>,
);

/// The listings a pending creation is adopted from: the panes, and the
/// workspaces herdr holds.
type Adoption = (
    Vec<emission_capsule_nexus::PaneListing>,
    Vec<emission_capsule_nexus::WorkspaceListing>,
);

impl Capsule {
    fn new(instance_id: signal_capsule_nexus::InstanceId) -> Self {
        Self {
            core: capsule_nexus_core::Core::new(capsule_nexus_core::InMemory::default()),
            principals: std::collections::HashMap::new(),
            reserved: std::collections::HashMap::new(),
            revisions: std::collections::HashMap::new(),
            next_reservation: 1,
            instance_id,
            next_creation_sequence: 1,
            subscriber_capacity: None,
        }
    }

    fn finish_reservation(&mut self, pane: &signal_capsule_nexus::Pane, token: u64) {
        let owns = self.reserved.get(pane).copied() == Some(token);
        assert!(owns, "only the reservation token owner may finalize it");
        self.reserved.remove(pane);
    }

    /// Reconciles a listing into Memory, after adopting the creations it
    /// shows, keeping any pane changed since the listing was captured.
    fn reconciled(
        &mut self,
        session: signal_capsule_nexus::Session,
        captured: std::collections::HashMap<signal_capsule_nexus::Pane, u64>,
        pending: Vec<capsule_nexus_core::generated::memory::PendingCreation>,
        adoption: Option<Adoption>,
        external: capsule_nexus_core::ExternalResult<Vec<emission_capsule_nexus::PaneObservation>>,
    ) -> signal_capsule_nexus::Outcome {
        if let Some((panes, workspaces)) = &adoption {
            adopt_pending_creations(self, &pending, panes, workspaces);
        }
        let protected = protect_newer_state(self, &session, &captured);
        let outcome = self.core.list(session.clone(), protected, external);
        if matches!(outcome, signal_capsule_nexus::Outcome::Listed(_)) {
            if let Ok(owned) = self.core.owned_in_session(&session) {
                for owned in owned {
                    bump_revision(&mut self.revisions, owned.pane);
                }
            }
        }
        outcome
    }
}

#[kameo::messages]
impl Capsule {
    /// Records a creation as pending under the next creation identifier.
    #[message]
    fn prepare_creation(
        &mut self,
        principal: shared_translator::Principal,
        workspace: signal_capsule_nexus::Workspace,
    ) -> Result<signal_capsule_nexus::CreationId, signal_capsule_nexus::Outcome> {
        let creation_id = signal_capsule_nexus::CreationId {
            instance_id: self.instance_id.clone(),
            sequence: signal_capsule_nexus::Sequence(self.next_creation_sequence),
        };
        let Some(next) = self.next_creation_sequence.checked_add(1) else {
            return Err(signal_capsule_nexus::Outcome::StoreRefused(signal_capsule_nexus::StoreRefusal {
                memory_refusal: signal_capsule_nexus::MemoryRefusal(signal_capsule_nexus::Message(
                    "creation sequence exhausted".into(),
                )),
                store_phase: signal_capsule_nexus::StorePhase::BeforeEmission,
            }));
        };
        self.core.prepare_creation(capsule_nexus_core::generated::memory::PendingCreation {
            creation_id: creation_id.clone(),
            principal,
            workspace,
        })?;
        self.next_creation_sequence = next;
        Ok(creation_id)
    }

    /// Joins a creation to the pane herdr made for it.
    #[message]
    fn finish_creation(
        &mut self,
        creation_id: signal_capsule_nexus::CreationId,
        principal: shared_translator::Principal,
        external: capsule_nexus_core::ExternalResult<signal_capsule_nexus::Pane>,
    ) -> Result<signal_capsule_nexus::Outcome, std::convert::Infallible> {
        let outcome = self.core.create(creation_id, external);
        if let signal_capsule_nexus::Outcome::Created(pane) = &outcome {
            self.revisions.insert(pane.clone(), 1);
            self.principals.insert(pane.clone(), principal);
        }
        Ok(outcome)
    }

    /// Reserves an owned pane for one attempt.
    #[message]
    fn reserve(
        &mut self,
        principal: shared_translator::Principal,
        pane: signal_capsule_nexus::Pane,
    ) -> Result<u64, signal_capsule_nexus::Outcome> {
        use signal_capsule_nexus::Outcome;
        self.core.preflight_owned(&pane)?;
        if self.principals.get(&pane) != Some(&principal) {
            return Err(Outcome::Unknown(pane));
        }
        if self.reserved.contains_key(&pane) {
            return Err(Outcome::AttemptInProgress(pane));
        }
        let token = self.next_reservation;
        self.next_reservation = self.next_reservation.wrapping_add(1).max(1);
        self.reserved.insert(pane, token);
        Ok(token)
    }

    #[message]
    fn finish_start(
        &mut self,
        pane: signal_capsule_nexus::Pane,
        token: u64,
        command: signal_capsule_nexus::Command,
        label: signal_capsule_nexus::Label,
        external: capsule_nexus_core::ExternalResult<signal_capsule_nexus::State>,
    ) -> Result<signal_capsule_nexus::Outcome, std::convert::Infallible> {
        self.finish_reservation(&pane, token);
        let outcome = self.core.start(pane.clone(), command, label, external);
        if matches!(outcome, signal_capsule_nexus::Outcome::Done | signal_capsule_nexus::Outcome::Uncertain) {
            bump_revision(&mut self.revisions, pane);
        }
        Ok(outcome)
    }

    #[message]
    fn finish_prompt(
        &mut self,
        pane: signal_capsule_nexus::Pane,
        token: u64,
        text: signal_capsule_nexus::Text,
        external: capsule_nexus_core::ExternalResult<()>,
    ) -> Result<signal_capsule_nexus::Outcome, std::convert::Infallible> {
        self.finish_reservation(&pane, token);
        let outcome = self.core.prompt(pane.clone(), text, external);
        if matches!(outcome, signal_capsule_nexus::Outcome::Done | signal_capsule_nexus::Outcome::Uncertain) {
            bump_revision(&mut self.revisions, pane);
        }
        Ok(outcome)
    }

    #[message]
    fn finish_press(
        &mut self,
        pane: signal_capsule_nexus::Pane,
        token: u64,
        keys: signal_capsule_nexus::Keys,
        external: capsule_nexus_core::ExternalResult<()>,
    ) -> Result<signal_capsule_nexus::Outcome, std::convert::Infallible> {
        self.finish_reservation(&pane, token);
        let outcome = self.core.press(pane.clone(), keys, external);
        if matches!(outcome, signal_capsule_nexus::Outcome::Done | signal_capsule_nexus::Outcome::Uncertain) {
            bump_revision(&mut self.revisions, pane);
        }
        Ok(outcome)
    }

    #[message]
    fn finish_close(
        &mut self,
        pane: signal_capsule_nexus::Pane,
        token: u64,
        external: capsule_nexus_core::ExternalResult<()>,
    ) -> Result<signal_capsule_nexus::Outcome, std::convert::Infallible> {
        self.finish_reservation(&pane, token);
        let outcome = self.core.close(pane.clone(), external);
        if outcome == signal_capsule_nexus::Outcome::Done {
            self.revisions.remove(&pane);
            self.principals.remove(&pane);
        } else if outcome == signal_capsule_nexus::Outcome::Uncertain {
            bump_revision(&mut self.revisions, pane);
        }
        Ok(outcome)
    }

    /// Captures what a listing of the session is reconciled against.
    #[message]
    fn capture_listing(
        &mut self,
        session: signal_capsule_nexus::Session,
    ) -> Result<Captured, signal_capsule_nexus::Outcome> {
        let owned = self.core.owned_in_session(&session)?;
        let pending = self.core.pending_in_session(&session)?;
        let owners = owned
            .iter()
            .filter_map(|owned| self.principals.get(&owned.pane).cloned())
            .chain(pending.iter().map(|creation| creation.principal.clone()))
            .collect();
        Ok((self.revisions.clone(), pending, owners))
    }

    /// Reconciles a principal's listing and answers it with that
    /// principal's panes.
    #[message]
    fn finish_listing(
        &mut self,
        principal: shared_translator::Principal,
        session: signal_capsule_nexus::Session,
        captured: std::collections::HashMap<signal_capsule_nexus::Pane, u64>,
        pending: Vec<capsule_nexus_core::generated::memory::PendingCreation>,
        adoption: Option<Adoption>,
        external: capsule_nexus_core::ExternalResult<Vec<emission_capsule_nexus::PaneObservation>>,
    ) -> Result<signal_capsule_nexus::Outcome, std::convert::Infallible> {
        let outcome = self.reconciled(session, captured, pending, adoption, external);
        Ok(filter_listing(outcome, &principal, &self.principals))
    }

    /// Reconciles the session's own observation, which feeds its subscribers.
    #[message]
    fn finish_observation(
        &mut self,
        session: signal_capsule_nexus::Session,
        captured: std::collections::HashMap<signal_capsule_nexus::Pane, u64>,
        pending: Vec<capsule_nexus_core::generated::memory::PendingCreation>,
        adoption: Option<Adoption>,
        external: capsule_nexus_core::ExternalResult<Vec<emission_capsule_nexus::PaneObservation>>,
    ) {
        self.reconciled(session, captured, pending, adoption, external);
    }

    /// Opens a stream on the session's Publisher, a new one made at the
    /// subscriber capacity the actor holds. The stream holds its place among
    /// the session's streams until it is dropped.
    #[message(ctx)]
    fn subscribe(
        &mut self,
        session: signal_capsule_nexus::Session,
        ctx: &mut kameo::message::Context<Self, Result<capsule_nexus_core::Subscription, signal_capsule_nexus::Outcome>>,
    ) -> Result<capsule_nexus_core::Subscription, signal_capsule_nexus::Outcome> {
        use futures_util::StreamExt;
        let capacity = self.subscriber_capacity.ok_or_else(|| {
            signal_capsule_nexus::Outcome::StoreRefused(signal_capsule_nexus::StoreRefusal {
                memory_refusal: signal_capsule_nexus::MemoryRefusal(signal_capsule_nexus::Message(
                    "subscriber capacity is not configured".into(),
                )),
                store_phase: signal_capsule_nexus::StorePhase::BeforeEmission,
            })
        })?;
        let capsule_nexus_core::Subscription { stream, feed } = self.core.subscribe(&session, capacity)?;
        let lease = StreamLease {
            capsule: ctx.actor_ref().clone(),
            session,
        };
        let frames = futures_util::stream::unfold((stream, lease), |(mut stream, lease)| async move {
            let frame = stream.next().await?;
            Some((frame, (stream, lease)))
        });
        let frames: std::pin::Pin<
            Box<
                dyn futures_util::Stream<
                        Item = signal::Frame<signal_capsule_nexus::Panes, signal_capsule_nexus::Known>,
                    > + Send,
            >,
        > = Box::pin(frames);
        Ok(capsule_nexus_core::Subscription {
            stream: signal::Stream::from(frames),
            feed,
        })
    }

    /// Holds the configured subscriber capacity for every Publisher made from
    /// now on, and moves every session's Publisher to it; each open stream
    /// follows it.
    #[message]
    fn resize_publishers(&mut self, capacity: nexus::Capacity) {
        self.subscriber_capacity = Some(capacity);
        self.core.resize(capacity);
    }

    /// One stream of the session was dropped.
    #[message]
    fn unsubscribe(&mut self, session: signal_capsule_nexus::Session) {
        self.core.unsubscribe(&session);
    }

    /// How many sessions have a Publisher.
    #[message]
    pub fn publication_count(&mut self) -> usize {
        self.core.publication_count()
    }
}

/// A stream's place among its session's streams, given back when the stream
/// is dropped, however its subscription ended.
struct StreamLease {
    capsule: kameo::actor::ActorRef<Capsule>,
    session: signal_capsule_nexus::Session,
}

impl Drop for StreamLease {
    fn drop(&mut self) {
        let _ = self
            .capsule
            .tell(Unsubscribe {
                session: self.session.clone(),
            })
            .try_send();
    }
}

/// The attempt an input makes, or for a Subscribe the session it opens on:
/// a subscription opens on the Publisher and changes nothing in Memory, so it
/// is no attempt.
fn operation(
    query: signal_capsule_nexus::Input,
) -> Result<capsule_nexus_core::generated::operation::Operation, signal_capsule_nexus::Session> {
    use capsule_nexus_core::generated::operation::{
        Operation, Pressing_Data, Prompting_Data, Starting_Data,
    };
    use signal_capsule_nexus::Input;
    Ok(match query {
        Input::Create(workspace) => Operation::Creating(workspace),
        Input::Start(start) => Operation::Starting(Starting_Data {
            pane: start.pane,
            command: start.command,
            label: start.label,
        }),
        Input::Prompt(prompt) => Operation::Prompting(Prompting_Data {
            pane: prompt.pane,
            text: prompt.text,
        }),
        Input::Press(press) => Operation::Pressing(Pressing_Data {
            pane: press.pane,
            keys: press.keys,
        }),
        Input::Close(pane) => Operation::Closing(pane),
        Input::List(session) => Operation::Listing(session),
        Input::Subscribe(session) => return Err(session),
    })
}

/// An actor step that can refuse: its refusal, or Uncertain when the actor
/// that owns the state is gone.
fn settled<T, M>(asked: Result<T, kameo::error::SendError<M, signal_capsule_nexus::Outcome>>) -> Result<T, signal_capsule_nexus::Outcome> {
    asked.map_err(|error| match error {
        kameo::error::SendError::HandlerError(outcome) => outcome,
        _ => signal_capsule_nexus::Outcome::Uncertain,
    })
}

/// An actor step that ends an attempt: its outcome, or Uncertain when the
/// actor that owns the state is gone.
fn concluded<M>(
    asked: Result<signal_capsule_nexus::Outcome, kameo::error::SendError<M, std::convert::Infallible>>,
) -> signal_capsule_nexus::Outcome {
    asked.unwrap_or(signal_capsule_nexus::Outcome::Uncertain)
}

impl Dispatcher {
    /// A dispatcher whose actor runs on `runtime`.
    pub fn new(
        paths: &SocketPaths,
        configuration: Arc<ConfigurationStore>,
        instance_id: signal_capsule_nexus::InstanceId,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        use kameo::actor::Spawn;
        let capsule = {
            let _entered = runtime.enter();
            Capsule::spawn_with_mailbox(Capsule::new(instance_id), kameo::mailbox::unbounded())
        };
        configuration.hold_publishers_in(capsule.clone());
        Self {
            capsule,
            emitter: Arc::new(SocketEmitter {
                lane: EmissionLane::from(paths.translator.clone()),
            }),
            configuration,
            runtime,
        }
    }

    /// Attempts one input as `principal` and answers it: once, or for a
    /// Subscribe with the stream of the session's panes. Each step on the
    /// state is one message to the actor; the herdr calls run between them,
    /// on this thread, which is not the runtime's.
    pub fn dispatch_as(
        &self,
        principal: shared_translator::Principal,
        query: signal_capsule_nexus::Input,
        configuration: Arc<ReadyCoreConfiguration>,
    ) -> signal_capsule_nexus::Answer {
        use emission_capsule_nexus::{
            AgentPrompt_Data, AgentStart_Data, Herdr, SendKeys_Data, Verb, WorkspaceCreate_Data,
        };
        use capsule_nexus_core::generated::operation::Operation;
        use signal_capsule_nexus::{Answer, Output};
        let attempt = match operation(query) {
            Ok(attempt) => attempt,
            Err(session) => return self.open_subscription(session),
        };
        let response = match attempt {
            Operation::Creating(workspace) => {
                let creation_id = match settled(
                    self.capsule
                        .ask(PrepareCreation {
                            principal: principal.clone(),
                            workspace: workspace.clone(),
                        })
                        .blocking_send(),
                ) {
                    Ok(creation_id) => creation_id,
                    Err(outcome) => return Answer::Once(Output::Created(outcome)),
                };
                let external = self.execute(
                    &configuration,
                    Herdr {
                        session: workspace.session.clone(),
                        verb: Verb::WorkspaceCreate(WorkspaceCreate_Data {
                            creation_id: creation_id.clone(),
                            root: workspace.root.clone(),
                            variable_vector: workspace.variable_vector.clone(),
                        }),
                    },
                );
                Output::Created(concluded(
                    self.capsule
                        .ask(FinishCreation {
                            creation_id,
                            principal,
                            external: created_result(external),
                        })
                        .blocking_send(),
                ))
            }
            Operation::Starting(start) => {
                let token = match self.reserve(&principal, &start.pane) {
                    Ok(token) => token,
                    Err(outcome) => return Answer::Once(Output::Started(outcome)),
                };
                let command = start.command.clone();
                let external = self.execute(
                    &configuration,
                    Herdr {
                        session: start.pane.session.clone(),
                        verb: Verb::AgentStart(AgentStart_Data {
                            pane_id: start.pane.pane_id.clone(),
                            command: start.command,
                            timeout: emission_capsule_nexus::Timeout(
                                i64::try_from(configuration.start_timeout.as_millis())
                                    .expect("validated i64 millisecond timeout"),
                            ),
                        }),
                    },
                );
                Output::Started(concluded(
                    self.capsule
                        .ask(FinishStart {
                            pane: start.pane,
                            token,
                            command,
                            label: start.label,
                            external: started_result(external),
                        })
                        .blocking_send(),
                ))
            }
            Operation::Prompting(prompt) => {
                let token = match self.reserve(&principal, &prompt.pane) {
                    Ok(token) => token,
                    Err(outcome) => return Answer::Once(Output::Prompted(outcome)),
                };
                let text = prompt.text.clone();
                let external = self.execute(
                    &configuration,
                    Herdr {
                        session: prompt.pane.session.clone(),
                        verb: Verb::AgentPrompt(AgentPrompt_Data {
                            pane_id: prompt.pane.pane_id.clone(),
                            text: prompt.text,
                        }),
                    },
                );
                Output::Prompted(concluded(
                    self.capsule
                        .ask(FinishPrompt {
                            pane: prompt.pane,
                            token,
                            text,
                            external: done_result(external),
                        })
                        .blocking_send(),
                ))
            }
            Operation::Pressing(press) => {
                let token = match self.reserve(&principal, &press.pane) {
                    Ok(token) => token,
                    Err(outcome) => return Answer::Once(Output::Pressed(outcome)),
                };
                let keys = press.keys.clone();
                let external = self.execute(
                    &configuration,
                    Herdr {
                        session: press.pane.session.clone(),
                        verb: Verb::SendKeys(SendKeys_Data {
                            pane_id: press.pane.pane_id.clone(),
                            keys: press.keys,
                        }),
                    },
                );
                Output::Pressed(concluded(
                    self.capsule
                        .ask(FinishPress {
                            pane: press.pane,
                            token,
                            keys,
                            external: done_result(external),
                        })
                        .blocking_send(),
                ))
            }
            Operation::Closing(pane) => {
                let token = match self.reserve(&principal, &pane) {
                    Ok(token) => token,
                    Err(outcome) => return Answer::Once(Output::Closed(outcome)),
                };
                let external = self.execute(
                    &configuration,
                    Herdr {
                        session: pane.session.clone(),
                        verb: Verb::PaneClose(pane.pane_id.clone()),
                    },
                );
                Output::Closed(concluded(
                    self.capsule
                        .ask(FinishClose {
                            pane,
                            token,
                            external: closed_result(external),
                        })
                        .blocking_send(),
                ))
            }
            Operation::Listing(session) => {
                let (captured, pending, owners) =
                    match settled(self.capsule.ask(CaptureListing { session: session.clone() }).blocking_send()) {
                        Ok(captured) => captured,
                        Err(outcome) => return Answer::Once(Output::Listed(outcome)),
                    };
                if !owners.contains(&principal) {
                    return Answer::Once(Output::Listed(signal_capsule_nexus::Outcome::Listed(Vec::new())));
                }
                let (adoption, external) = self.observed(&configuration, &session, &pending);
                Output::Listed(concluded(
                    self.capsule
                        .ask(FinishListing {
                            principal,
                            session,
                            captured,
                            pending,
                            adoption,
                            external,
                        })
                        .blocking_send(),
                ))
            }
        };
        Answer::Once(response)
    }

    /// Opens a stream on the session's Publisher, whoever asks: observing is
    /// open, acting stays the owner's. The stream that makes the Publisher
    /// starts the session's feed, which lives as long as the Publisher.
    fn open_subscription(&self, session: signal_capsule_nexus::Session) -> signal_capsule_nexus::Answer {
        use signal_capsule_nexus::{Answer, Output};
        match settled(self.capsule.ask(Subscribe { session: session.clone() }).blocking_send()) {
            Ok(subscription) => {
                if let Some(ended) = subscription.feed {
                    self.runtime.spawn(self.clone().feed(session, ended));
                }
                Answer::Subscribed(subscription.stream)
            }
            Err(outcome) => Answer::Once(Output::SubscribeRefused(outcome)),
        }
    }

    /// The session's feed: one observation of the session every polling
    /// interval, reconciled into Memory, whose changes the Publisher hands
    /// to the subscribers, until the Publisher is dropped.
    async fn feed(self, session: signal_capsule_nexus::Session, mut ended: tokio::sync::oneshot::Receiver<()>) {
        loop {
            let Some(configuration) = self.configuration.snapshot() else {
                return;
            };
            tokio::select! {
                biased;
                _ = &mut ended => return,
                () = tokio::time::sleep(configuration.polling) => {}
            }
            let observing = self.clone();
            let observed = session.clone();
            if tokio::task::spawn_blocking(move || observing.observe(observed, configuration))
                .await
                .is_err()
            {
                return;
            }
        }
    }

    /// Observes the whole session, whoever owns its panes, and reconciles
    /// the observation into Memory. A session with nothing owned or pending
    /// is not observed.
    fn observe(&self, session: signal_capsule_nexus::Session, configuration: Arc<ReadyCoreConfiguration>) {
        let Ok((captured, pending, owners)) =
            settled(self.capsule.ask(CaptureListing { session: session.clone() }).blocking_send())
        else {
            return;
        };
        if owners.is_empty() {
            return;
        }
        let (adoption, external) = self.observed(&configuration, &session, &pending);
        let _ = self
            .capsule
            .ask(FinishObservation {
                session,
                captured,
                pending,
                adoption,
                external,
            })
            .blocking_send();
    }

    /// The herdr calls a listing makes: the panes, then the agents, and the
    /// workspaces while a creation is pending.
    fn observed(
        &self,
        configuration: &ReadyCoreConfiguration,
        session: &signal_capsule_nexus::Session,
        pending: &[capsule_nexus_core::generated::memory::PendingCreation],
    ) -> (
        Option<Adoption>,
        capsule_nexus_core::ExternalResult<Vec<emission_capsule_nexus::PaneObservation>>,
    ) {
        use emission_capsule_nexus::{Herdr, Verb};
        let panes = self.execute(
            configuration,
            Herdr {
                session: session.clone(),
                verb: Verb::PaneList,
            },
        );
        let (pane_listings, external) = match pane_listing_result(panes) {
            Ok(panes) => {
                let agents = self.execute(
                    configuration,
                    Herdr {
                        session: session.clone(),
                        verb: Verb::AgentList,
                    },
                );
                let external = join_listings(panes.clone(), agent_listing_result(agents));
                (Some(panes), external)
            }
            Err(result) => (None, result),
        };
        let workspace_listings = if pending.is_empty() {
            None
        } else {
            workspace_listing_result(self.execute(
                configuration,
                Herdr {
                    session: session.clone(),
                    verb: Verb::WorkspaceList,
                },
            ))
            .ok()
        };
        (pane_listings.zip(workspace_listings), external)
    }

    pub fn peer_grant_is_current(
        &self,
        caller: &shared_translator::Caller,
        peer_id: &shared_translator::PeerId,
        generation: u64,
    ) -> bool {
        self.configuration
            .peer_grant_is_current(caller, peer_id, generation)
    }

    fn reserve(
        &self,
        principal: &shared_translator::Principal,
        pane: &signal_capsule_nexus::Pane,
    ) -> Result<u64, signal_capsule_nexus::Outcome> {
        settled(
            self.capsule
                .ask(Reserve {
                    principal: principal.clone(),
                    pane: pane.clone(),
                })
                .blocking_send(),
        )
    }

    fn execute(
        &self,
        configuration: &ReadyCoreConfiguration,
        herdr: emission_capsule_nexus::Herdr,
    ) -> EmissionResult {
        self.emitter.emit(configuration, herdr)
    }
}

impl ExternalEmitter for SocketEmitter {
    fn emit(
        &self,
        configuration: &ReadyCoreConfiguration,
        herdr: emission_capsule_nexus::Herdr,
    ) -> EmissionResult {
        let emitted = self.lane.emit::<_, emission_capsule_nexus::Output>(
            &emission_capsule_nexus::Input::Emit(herdr),
            configuration.transport_timeout,
            configuration.frame_capacity_bytes,
        );
        match emitted {
            Ok(emission_capsule_nexus::Output::Emitted(outcome)) => EmissionResult::External(outcome),
            Err(EmissionFailure::Unsubmitted(message)) => {
                unavailable_transport(self.lane.translator_socket(), message)
            }
            Err(EmissionFailure::Unanswered(_)) => {
                EmissionResult::External(emission_capsule_nexus::ExternalOutcome::Uncertain)
            }
        }
    }
}

fn bump_revision(
    revisions: &mut std::collections::HashMap<signal_capsule_nexus::Pane, u64>,
    pane: signal_capsule_nexus::Pane,
) {
    let next = revisions
        .get(&pane)
        .copied()
        .unwrap_or(0)
        .wrapping_add(1)
        .max(1);
    revisions.insert(pane, next);
}

fn protect_newer_state(
    state: &Capsule,
    session: &signal_capsule_nexus::Session,
    captured: &std::collections::HashMap<signal_capsule_nexus::Pane, u64>,
) -> capsule_nexus_core::Protected {
    let Ok(owned) = state.core.owned_in_session(session) else {
        return capsule_nexus_core::Protected::default();
    };
    capsule_nexus_core::Protected(
        owned
            .into_iter()
            .filter(|current| {
                captured.get(&current.pane) != state.revisions.get(&current.pane)
                    || state.reserved.contains_key(&current.pane)
            })
            .map(|current| current.pane)
            .collect(),
    )
}

fn filter_listing(
    outcome: signal_capsule_nexus::Outcome,
    principal: &shared_translator::Principal,
    principals: &std::collections::HashMap<signal_capsule_nexus::Pane, shared_translator::Principal>,
) -> signal_capsule_nexus::Outcome {
    match outcome {
        signal_capsule_nexus::Outcome::Listed(mut known) => {
            known.retain(|item| principals.get(&item.pane) == Some(principal));
            signal_capsule_nexus::Outcome::Listed(known)
        }
        other => other,
    }
}

fn unavailable_transport(path: &std::path::Path, message: String) -> EmissionResult {
    EmissionResult::Unavailable(signal_capsule_nexus::TransportRefusal {
        socket_path: signal_capsule_nexus::SocketPath(path.display().to_string()),
        message: signal_capsule_nexus::Message(message),
    })
}

fn done_result(outcome: EmissionResult) -> capsule_nexus_core::ExternalResult<()> {
    match outcome {
        EmissionResult::External(emission_capsule_nexus::ExternalOutcome::Done) => {
            capsule_nexus_core::ExternalResult::Succeeded(())
        }
        EmissionResult::External(emission_capsule_nexus::ExternalOutcome::Refused(refusal)) => {
            capsule_nexus_core::ExternalResult::Refused(refusal)
        }
        EmissionResult::Unavailable(refusal) => {
            capsule_nexus_core::ExternalResult::Unavailable(refusal)
        }
        _ => capsule_nexus_core::ExternalResult::Uncertain,
    }
}

fn is_absent_close(outcome: &EmissionResult) -> bool {
    matches!(
        outcome,
        EmissionResult::External(emission_capsule_nexus::ExternalOutcome::Refused(refusal))
            if refusal.error_code.0 == "pane_not_found"
    )
}

fn closed_result(outcome: EmissionResult) -> capsule_nexus_core::ExternalResult<()> {
    if is_absent_close(&outcome) {
        capsule_nexus_core::ExternalResult::Succeeded(())
    } else {
        done_result(outcome)
    }
}

fn started_result(
    outcome: EmissionResult,
) -> capsule_nexus_core::ExternalResult<signal_capsule_nexus::State> {
    match outcome {
        EmissionResult::External(emission_capsule_nexus::ExternalOutcome::Started(state)) => {
            capsule_nexus_core::ExternalResult::Succeeded(state)
        }
        EmissionResult::External(emission_capsule_nexus::ExternalOutcome::Refused(refusal)) => {
            capsule_nexus_core::ExternalResult::Refused(refusal)
        }
        EmissionResult::Unavailable(refusal) => {
            capsule_nexus_core::ExternalResult::Unavailable(refusal)
        }
        _ => capsule_nexus_core::ExternalResult::Uncertain,
    }
}

fn created_result(
    outcome: EmissionResult,
) -> capsule_nexus_core::ExternalResult<signal_capsule_nexus::Pane> {
    match outcome {
        EmissionResult::External(emission_capsule_nexus::ExternalOutcome::Created(pane)) => {
            capsule_nexus_core::ExternalResult::Succeeded(pane)
        }
        EmissionResult::External(emission_capsule_nexus::ExternalOutcome::Refused(refusal)) => {
            capsule_nexus_core::ExternalResult::Refused(refusal)
        }
        EmissionResult::Unavailable(refusal) => {
            capsule_nexus_core::ExternalResult::Unavailable(refusal)
        }
        _ => capsule_nexus_core::ExternalResult::Uncertain,
    }
}

fn pane_listing_result(
    outcome: EmissionResult,
) -> Result<
    Vec<emission_capsule_nexus::PaneListing>,
    capsule_nexus_core::ExternalResult<Vec<emission_capsule_nexus::PaneObservation>>,
> {
    match outcome {
        EmissionResult::External(emission_capsule_nexus::ExternalOutcome::Panes(observed)) => Ok(observed),
        EmissionResult::External(emission_capsule_nexus::ExternalOutcome::Refused(refusal)) => {
            Err(capsule_nexus_core::ExternalResult::Refused(refusal))
        }
        EmissionResult::Unavailable(refusal) => {
            Err(capsule_nexus_core::ExternalResult::Unavailable(refusal))
        }
        _ => Err(capsule_nexus_core::ExternalResult::Uncertain),
    }
}

fn agent_listing_result(
    outcome: EmissionResult,
) -> Result<
    Vec<emission_capsule_nexus::AgentListing>,
    capsule_nexus_core::ExternalResult<Vec<emission_capsule_nexus::PaneObservation>>,
> {
    match outcome {
        EmissionResult::External(emission_capsule_nexus::ExternalOutcome::Agents(observed)) => Ok(observed),
        EmissionResult::External(emission_capsule_nexus::ExternalOutcome::Refused(refusal)) => {
            Err(capsule_nexus_core::ExternalResult::Refused(refusal))
        }
        EmissionResult::Unavailable(refusal) => {
            Err(capsule_nexus_core::ExternalResult::Unavailable(refusal))
        }
        _ => Err(capsule_nexus_core::ExternalResult::Uncertain),
    }
}

fn workspace_listing_result(
    outcome: EmissionResult,
) -> Result<
    Vec<emission_capsule_nexus::WorkspaceListing>,
    capsule_nexus_core::ExternalResult<Vec<emission_capsule_nexus::PaneObservation>>,
> {
    match outcome {
        EmissionResult::External(emission_capsule_nexus::ExternalOutcome::Workspaces(observed)) => {
            Ok(observed)
        }
        EmissionResult::External(emission_capsule_nexus::ExternalOutcome::Refused(refusal)) => {
            Err(capsule_nexus_core::ExternalResult::Refused(refusal))
        }
        EmissionResult::Unavailable(refusal) => {
            Err(capsule_nexus_core::ExternalResult::Unavailable(refusal))
        }
        _ => Err(capsule_nexus_core::ExternalResult::Uncertain),
    }
}

fn adopt_pending_creations(
    state: &mut Capsule,
    pending: &[capsule_nexus_core::generated::memory::PendingCreation],
    panes: &[emission_capsule_nexus::PaneListing],
    workspaces: &[emission_capsule_nexus::WorkspaceListing],
) {
    for creation in pending {
        let matching_workspaces: Vec<_> = workspaces
            .iter()
            .filter(|workspace| {
                emission_capsule_nexus::managed_creation_id(&workspace.workspace_label).as_ref()
                    == Some(&creation.creation_id)
            })
            .collect();
        if matching_workspaces.len() != 1 {
            continue;
        }
        let matching_panes: Vec<_> = panes
            .iter()
            .filter(|pane| pane.workspace_id == matching_workspaces[0].workspace_id)
            .collect();
        if matching_panes.len() != 1 {
            continue;
        }
        let pane = matching_panes[0].pane.clone();
        if matches!(
            state.core.create(
                creation.creation_id.clone(),
                capsule_nexus_core::ExternalResult::Succeeded(pane.clone()),
            ),
            signal_capsule_nexus::Outcome::Created(_)
        ) {
            state.principals.insert(pane.clone(), creation.principal.clone());
            state.revisions.insert(pane, 1);
        }
    }
}

fn join_listings(
    panes: Vec<emission_capsule_nexus::PaneListing>,
    agents: Result<
        Vec<emission_capsule_nexus::AgentListing>,
        capsule_nexus_core::ExternalResult<Vec<emission_capsule_nexus::PaneObservation>>,
    >,
) -> capsule_nexus_core::ExternalResult<Vec<emission_capsule_nexus::PaneObservation>> {
    use std::collections::{HashMap, HashSet};
    let agents = match agents {
        Ok(agents) => agents,
        Err(result) => return result,
    };
    let mut pane_ids = HashSet::new();
    let panes_by_id: HashMap<_, _> = panes
        .iter()
        .filter_map(|pane| {
            pane_ids
                .insert(pane.pane.clone())
                .then_some((pane.pane.clone(), pane))
        })
        .collect();
    if panes_by_id.len() != panes.len() {
        return capsule_nexus_core::ExternalResult::Uncertain;
    }
    let mut agents_by_id = HashMap::new();
    for agent in agents {
        let Some(pane) = panes_by_id.get(&agent.pane) else {
            return capsule_nexus_core::ExternalResult::Uncertain;
        };
        if agent.terminal_id.0 != pane.terminal_id.0
            || agent.workspace_id.0 != pane.workspace_id.0
            || agent.tab_id.0 != pane.tab_id.0
            || agents_by_id.insert(agent.pane.clone(), agent).is_some()
        {
            return capsule_nexus_core::ExternalResult::Uncertain;
        }
    }
    capsule_nexus_core::ExternalResult::Succeeded(
        panes
            .into_iter()
            .map(|pane| emission_capsule_nexus::PaneObservation {
                state: agents_by_id
                    .remove(&pane.pane)
                    .map_or(signal_capsule_nexus::State::Open, |agent| agent.state),
                pane: pane.pane,
            })
            .collect(),
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadyCoreConfiguration {
    pub polling: Duration,
    pub start_timeout: Duration,
    pub frame_capacity_bytes: usize,
    pub transport_timeout: Duration,
}

#[derive(Clone, Default)]
struct PartialConfiguration {
    polling: Option<Duration>,
    start_timeout: Option<Duration>,
    frame_capacity_bytes: Option<usize>,
    transport_timeout: Option<Duration>,
    subscriber_capacity: Option<usize>,
}

#[derive(Default)]
pub struct ConfigurationStore {
    configuration_transaction: std::sync::Mutex<()>,
    partial: RwLock<PartialConfiguration>,
    current: RwLock<Option<Arc<ReadyCoreConfiguration>>>,
    peer_subscriptions: std::sync::Mutex<std::collections::HashMap<shared_translator::PeerId, Vec<std::sync::Weak<SubscriptionClosing>>>>,
    peers: std::sync::Mutex<std::collections::HashMap<shared_translator::PeerId, BoundPeer>>,
    next_peer_generation: std::sync::atomic::AtomicU64,
    /// The actor that holds every session's Publisher, told each subscriber
    /// capacity configured.
    capsule: std::sync::OnceLock<kameo::actor::ActorRef<Capsule>>,
}

struct BoundPeer {
    live: shared_translator::unix::LivePeer,
    generation: u64,
}

impl BoundPeer {
    fn matches(&self, caller: &shared_translator::Caller) -> bool {
        self.live.matches_caller(caller)
    }
}

impl ConfigurationStore {
    fn peer_for(
        &self,
        caller: &shared_translator::Caller,
    ) -> Option<(shared_translator::PeerId, u64)> {
        let peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
        peers.iter().find_map(|(peer_id, peer)| {
            if peer.live.identity.user_id != caller.user_id
                || peer.live.identity.process_id != caller.process_id
            {
                return None;
            }
            peer.matches(caller).then(|| (peer_id.clone(), peer.generation))
        })
    }

    fn peer_grant_is_current(
        &self,
        caller: &shared_translator::Caller,
        peer_id: &shared_translator::PeerId,
        generation: u64,
    ) -> bool {
        self.peers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(peer_id)
            .is_some_and(|peer| peer.generation == generation && peer.matches(caller))
    }

    pub fn configure(
        &self,
        setting: core_meta_capsule_nexus::CoreSet,
        bind_public: impl FnOnce() -> Result<(), meta_signal_capsule_nexus::BootstrapRefusal>,
    ) -> core_meta_capsule_nexus::Output {
        use core_meta_capsule_nexus::Output;
        use meta_signal_capsule_nexus::{
            ConfigurationRefusal, Invalid_Data, Message, Owner, SettingName,
        };
        let _transaction = self
            .configuration_transaction
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if matches!(setting, core_meta_capsule_nexus::CoreSet::Peer(_) | core_meta_capsule_nexus::CoreSet::RemovePeer(_)) {
            return self.configure_peer(setting);
        }
        let mut candidate = self
            .partial
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let applied = setting.clone();
        let (name, valid) = match setting {
            core_meta_capsule_nexus::CoreSet::StartTimeout(value) => (
                SettingName::StartTimeout,
                checked_duration(value.0)
                    .map(|value| candidate.start_timeout = Some(value))
                    .is_some(),
            ),
            core_meta_capsule_nexus::CoreSet::Polling(value) => (
                SettingName::Polling,
                checked_duration(value.0)
                    .map(|value| candidate.polling = Some(value))
                    .is_some(),
            ),
            core_meta_capsule_nexus::CoreSet::FrameCapacity(value) => (
                SettingName::FrameCapacity,
                usize::try_from(value.0)
                    .ok()
                    .filter(|value| {
                        (capsule_nexus_wire::frame::HEADER_BYTES..=signal::MAXIMUM_SIGNAL_BYTES)
                            .contains(value)
                    })
                    .map(|value| candidate.frame_capacity_bytes = Some(value))
                    .is_some(),
            ),
            core_meta_capsule_nexus::CoreSet::TransportTimeout(value) => (
                SettingName::TransportTimeout,
                checked_duration(value.0)
                    .map(|value| candidate.transport_timeout = Some(value))
                    .is_some(),
            ),
            core_meta_capsule_nexus::CoreSet::SubscriberCapacity(value) => (
                SettingName::SubscriberCapacity,
                usize::try_from(value)
                    .ok()
                    .filter(|value| *value > 0)
                    .map(|value| candidate.subscriber_capacity = Some(value))
                    .is_some(),
            ),
            core_meta_capsule_nexus::CoreSet::Peer(_)
            | core_meta_capsule_nexus::CoreSet::RemovePeer(_) => (
                SettingName::Peer,
                false,
            ),
        };
        if !valid {
            return Output::ConfigureRefused(ConfigurationRefusal::Invalid(Invalid_Data {
                owner: Owner::Nexus,
                setting_name: name,
                message: Message(
                    "invalid setting value".into(),
                ),
            }));
        }
        let readiness = readiness(&candidate);
        let validated = match (
            &candidate.polling,
            &candidate.start_timeout,
            &candidate.frame_capacity_bytes,
            &candidate.transport_timeout,
            &candidate.subscriber_capacity,
        ) {
            (
                Some(polling),
                Some(start_timeout),
                Some(frame_capacity_bytes),
                Some(transport_timeout),
                Some(_),
            ) => Some(ReadyCoreConfiguration {
                polling: *polling,
                start_timeout: *start_timeout,
                frame_capacity_bytes: *frame_capacity_bytes,
                transport_timeout: *transport_timeout,
            }),
            _ => None,
        };
        if validated.is_some()
            && self
                .current
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .is_none()
        {
            if let Err(refusal) = bind_public() {
                return Output::ConfigureRefused(ConfigurationRefusal::Unavailable(refusal));
            }
        }
        *self.partial.write().unwrap_or_else(|p| p.into_inner()) = candidate;
        if let Some(validated) = validated {
            *self.current.write().unwrap_or_else(|p| p.into_inner()) = Some(Arc::new(validated));
        }
        if let core_meta_capsule_nexus::CoreSet::SubscriberCapacity(capacity) = &applied {
            self.resize_publishers(nexus::Capacity(*capacity));
        }
        Output::Configured(core_meta_capsule_nexus::Configured_Data {
            core_set: applied,
            readiness,
        })
    }

    fn configure_peer(
        &self,
        setting: core_meta_capsule_nexus::CoreSet,
    ) -> core_meta_capsule_nexus::Output {
        use core_meta_capsule_nexus::Output;
        use meta_signal_capsule_nexus::{ConfigurationRefusal, Readiness};
        let applied = setting.clone();
        match setting {
            core_meta_capsule_nexus::CoreSet::Peer(binding) => {
                let caller = shared_translator::Caller {
                    user_id: binding.peer_identity.user_id,
                    process_id: binding.peer_identity.process_id,
                };
                let live = match shared_translator::unix::LivePeer::capture(binding.peer_identity) {
                    Ok(live) => live,
                    Err(error) => {
                        return Output::ConfigureRefused(invalid_peer(&format!(
                            "peer process is unavailable: {error}"
                        )));
                    }
                };
                let mut peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
                if peers.iter().any(|(peer_id, peer)| {
                    peer_id != &binding.peer_id && peer.matches(&caller)
                }) {
                    return Output::ConfigureRefused(invalid_peer(
                        "peer process identity is already bound to another peer",
                    ));
                }
                let generation = self
                    .next_peer_generation
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    .wrapping_add(1);
                let replaced = peers
                    .insert(binding.peer_id, BoundPeer {
                        live,
                        generation,
                    });
                if replaced.is_some() {
                    self.close_peer_subscriptions(&applied);
                }
            }
            core_meta_capsule_nexus::CoreSet::RemovePeer(peer_id) => {
                let mut peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
                if peers.remove(&peer_id).is_none()
                {
                    return Output::ConfigureRefused(ConfigurationRefusal::UnknownPeer(peer_id));
                }
                self.close_peer_subscriptions(&applied);
            }
            _ => {
                return Output::ConfigureRefused(ConfigurationRefusal::Invalid(
                    meta_signal_capsule_nexus::Invalid_Data {
                        owner: meta_signal_capsule_nexus::Owner::Nexus,
                        setting_name: meta_signal_capsule_nexus::SettingName::Peer,
                        message: meta_signal_capsule_nexus::Message(
                            "a non-peer setting reached peer configuration".into(),
                        ),
                    },
                ))
            }
        }
        let ready = self.current.read().unwrap_or_else(|p| p.into_inner());
        Output::Configured(core_meta_capsule_nexus::Configured_Data {
            core_set: applied,
            readiness: if ready.is_some() { Readiness::Ready } else { readiness(&self.partial.read().unwrap_or_else(|p| p.into_inner())) },
        })
    }

    fn close_peer_subscriptions(&self, setting: &core_meta_capsule_nexus::CoreSet) {
        let peer_id = match setting {
            core_meta_capsule_nexus::CoreSet::Peer(binding) => &binding.peer_id,
            core_meta_capsule_nexus::CoreSet::RemovePeer(peer_id) => peer_id,
            _ => return,
        };
        if let Some(subscriptions) = self.peer_subscriptions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(peer_id)
        {
            for closing in subscriptions.into_iter().filter_map(|closing| closing.upgrade()) {
                closing.close(Closing::Withdrawn);
            }
        }
    }

    fn register_peer_subscriber(&self, peer_id: &shared_translator::PeerId, generation: u64, closing: &Arc<SubscriptionClosing>) -> bool {
        self.register_peer_subscriber_after_validation(peer_id, generation, closing, || {})
    }

    fn register_peer_subscriber_after_validation(
        &self,
        peer_id: &shared_translator::PeerId,
        generation: u64,
        closing: &Arc<SubscriptionClosing>,
        after_validation: impl FnOnce(),
    ) -> bool {
        let peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
        if peers.get(peer_id).map(|peer| peer.generation) != Some(generation) {
            closing.close(Closing::Withdrawn);
            return false;
        }
        // Rebinding takes the same peers lock before it removes and closes the
        // old generation's subscriptions. Retaining this guard through
        // insertion makes validation and registration one transaction with
        // invalidation.
        after_validation();
        self.peer_subscriptions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(peer_id.clone())
            .or_default()
            .push(Arc::downgrade(closing));
        true
    }

    /// Tells the actor that holds the Publishers each subscriber capacity
    /// configured from now on, and the capacity configured before it.
    fn hold_publishers_in(&self, capsule: kameo::actor::ActorRef<Capsule>) {
        let _transaction = self
            .configuration_transaction
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let configured = self
            .partial
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .subscriber_capacity;
        if self.capsule.set(capsule).is_ok() {
            if let Some(capacity) = configured {
                self.resize_publishers(nexus::Capacity(
                    i64::try_from(capacity).expect("validated i64 subscriber capacity"),
                ));
            }
        }
    }

    /// Moves every session's Publisher to the capacity, in the actor's turn.
    /// An actor that is gone holds no Publisher to move.
    fn resize_publishers(&self, capacity: nexus::Capacity) {
        if let Some(capsule) = self.capsule.get() {
            let _ = capsule.tell(ResizePublishers { capacity }).try_send();
        }
    }

    pub fn snapshot(&self) -> Option<Arc<ReadyCoreConfiguration>> {
        self.current
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

}

fn readiness(candidate: &PartialConfiguration) -> meta_signal_capsule_nexus::Readiness {
    use meta_signal_capsule_nexus::{Readiness, SettingName};
    let mut awaiting = Vec::new();
    if candidate.start_timeout.is_none() {
        awaiting.push(SettingName::StartTimeout);
    }
    if candidate.polling.is_none() {
        awaiting.push(SettingName::Polling);
    }
    if candidate.frame_capacity_bytes.is_none() {
        awaiting.push(SettingName::FrameCapacity);
    }
    if candidate.transport_timeout.is_none() {
        awaiting.push(SettingName::TransportTimeout);
    }
    if candidate.subscriber_capacity.is_none() {
        awaiting.push(SettingName::SubscriberCapacity);
    }
    if awaiting.is_empty() {
        Readiness::Ready
    } else {
        Readiness::Awaiting(awaiting)
    }
}

fn checked_duration(milliseconds: i64) -> Option<Duration> {
    let duration = Duration::from_millis(u64::try_from(milliseconds).ok()?);
    if duration.is_zero() || Instant::now().checked_add(duration).is_none() {
        None
    } else {
        Some(duration)
    }
}

fn invalid_peer(message: &str) -> meta_signal_capsule_nexus::ConfigurationRefusal {
    meta_signal_capsule_nexus::ConfigurationRefusal::Invalid(
        meta_signal_capsule_nexus::Invalid_Data {
            owner: meta_signal_capsule_nexus::Owner::Nexus,
            setting_name: meta_signal_capsule_nexus::SettingName::Peer,
            message: meta_signal_capsule_nexus::Message(message.into()),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    trait AnsweredForTest {
        fn answered(self) -> Option<signal_capsule_nexus::Output>;
    }

    impl AnsweredForTest for signal_capsule_nexus::Answer {
        fn answered(self) -> Option<signal_capsule_nexus::Output> {
            match self {
                Self::Once(response) => Some(response),
                Self::Subscribed(_) => None,
            }
        }
    }

    /// The runtime every test's actor, connections and feeds run on.
    fn test_runtime() -> &'static tokio::runtime::Runtime {
        static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
        RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("test runtime")
        })
    }

    fn login() -> shared_translator::Principal {
        shared_translator::Principal::Login(shared_translator::UserId(i64::from(
            nix::unistd::getuid().as_raw(),
        )))
    }

    trait DispatcherForTest {
        fn with_emitter(paths: &SocketPaths, emitter: Arc<dyn ExternalEmitter>) -> Dispatcher;
        fn dispatch(
            &self,
            query: signal_capsule_nexus::Input,
            configuration: Arc<ReadyCoreConfiguration>,
        ) -> signal_capsule_nexus::Output;
        fn subscription_source_count(&self) -> usize;
        fn subscribe(
            &self,
            session: signal_capsule_nexus::Session,
            configuration: Arc<ReadyCoreConfiguration>,
        ) -> std::os::unix::net::UnixStream;
        fn subscribe_as(
            &self,
            authority: PublicAuthority,
            user_id: shared_translator::UserId,
            session: signal_capsule_nexus::Session,
            configuration: Arc<ReadyCoreConfiguration>,
        ) -> std::os::unix::net::UnixStream;
    }

    impl DispatcherForTest for Dispatcher {
        /// The shipped constructor, given a configuration with short timings.
        fn with_emitter(paths: &SocketPaths, emitter: Arc<dyn ExternalEmitter>) -> Dispatcher {
            let configuration = Arc::new(ConfigurationStore::default());
            *configuration.current.write().expect("configuration") =
                Some(test_configuration());
            let mut dispatcher = Dispatcher::new(
                paths,
                configuration,
                signal_capsule_nexus::InstanceId("test-instance".into()),
                test_runtime().handle().clone(),
            );
            dispatcher.emitter = emitter;
            assert!(matches!(
                dispatcher.configuration.configure(core_meta_capsule_nexus::CoreSet::SubscriberCapacity(8), || Ok(())),
                core_meta_capsule_nexus::Output::Configured(_)
            ));
            dispatcher
        }

        fn dispatch(
            &self,
            query: signal_capsule_nexus::Input,
            configuration: Arc<ReadyCoreConfiguration>,
        ) -> signal_capsule_nexus::Output {
            self.dispatch_as(login(), query, configuration)
                .answered()
                .expect("test helper received a streaming subscription")
        }

        fn subscription_source_count(&self) -> usize {
            self.capsule
                .ask(PublicationCount)
                .blocking_send()
                .expect("the actor answers")
        }

        /// A greeted connection whose first exchange is a Subscribe from the
        /// login, served as the public socket serves it; the client end, with
        /// the greeting read.
        fn subscribe(
            &self,
            session: signal_capsule_nexus::Session,
            configuration: Arc<ReadyCoreConfiguration>,
        ) -> std::os::unix::net::UnixStream {
            self.subscribe_as(
                PublicAuthority::Translator,
                shared_translator::UserId(i64::from(nix::unistd::getuid().as_raw())),
                session,
                configuration,
            )
        }

        /// A Subscribe whose Call names `user_id`, on a connection served under
        /// `authority`.
        fn subscribe_as(
            &self,
            authority: PublicAuthority,
            user_id: shared_translator::UserId,
            session: signal_capsule_nexus::Session,
            configuration: Arc<ReadyCoreConfiguration>,
        ) -> std::os::unix::net::UnixStream {
            use nexus_querying::{Patience, Querying, QueryingConnection};
            use capsule_nexus_wire::public_call::{Call_Data, Input, Output};
            let (server, client) = std::os::unix::net::UnixStream::pair().expect("pair");
            let caller = capsule_nexus_wire::frame::peer_caller(&client).expect("caller");
            test_runtime().spawn(PublicConnection::serve(
                server,
                authority,
                self.clone(),
                configuration,
            ));
            let mut querying = QueryingConnection::<Input, Output>::greeted(
                client.try_clone().expect("clone"),
                Patience::Progress(Duration::from_millis(500)),
                signal::FrameCapacity::from(4096),
            )
            .expect("greeted");
            let exchange = querying
                .open(Input::Call(Call_Data {
                    caller: shared_translator::Caller { user_id, ..caller },
                    public_input: capsule_nexus_wire::public_payload::Input(
                        signal_capsule_nexus::Input::Subscribe(session),
                    ),
                }))
                .expect("subscribe");
            assert_eq!(exchange, signal::FIRST_EXCHANGE);
            client
        }
    }

    struct CountingEmitter(std::sync::atomic::AtomicUsize);

    impl ExternalEmitter for CountingEmitter {
        fn emit(
            &self,
            _configuration: &ReadyCoreConfiguration,
            _herdr: emission_capsule_nexus::Herdr,
        ) -> EmissionResult {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            EmissionResult::External(emission_capsule_nexus::ExternalOutcome::Uncertain)
        }
    }

    fn test_paths() -> SocketPaths {
        SocketPaths::from(&shared_translator::placement::Place {
            place_name: shared_translator::placement::PlaceName("herdr-test".into()),
            runtime: shared_translator::placement::Runtime("/tmp/herdr-test".into()),
            state: shared_translator::placement::State("/tmp/herdr-test".into()),
        })
    }

    fn test_configuration() -> Arc<ReadyCoreConfiguration> {
        Arc::new(ReadyCoreConfiguration {
            polling: Duration::from_millis(10),
            start_timeout: Duration::from_millis(20),
            frame_capacity_bytes: 4096,
            transport_timeout: Duration::from_millis(30),
        })
    }

    fn test_pane() -> signal_capsule_nexus::Pane {
        signal_capsule_nexus::Pane {
            session: signal_capsule_nexus::Session("missing".into()),
            pane_id: signal_capsule_nexus::PaneId("pane".into()),
        }
    }

    #[test]
    fn peer_setting_captures_process_lifetime_and_remove_is_typed() {
        use core_meta_capsule_nexus::{CoreSet, Output};
        use shared_translator::{PeerBinding, PeerId, PeerIdentity, ProcessId, UserId};

        let store = ConfigurationStore::default();
        let peer_id = PeerId("fixture".into());
        let binding = PeerBinding {
            peer_id: peer_id.clone(),
            peer_identity: PeerIdentity {
                user_id: UserId(i64::from(nix::unistd::getuid().as_raw())),
                process_id: ProcessId(i64::from(std::process::id())),
            },
        };
        assert!(matches!(
            store.configure(CoreSet::Peer(binding), || Ok(())),
            Output::Configured(_)
        ));
        assert_eq!(
            store.peer_for(&shared_translator::Caller {
                user_id: UserId(i64::from(nix::unistd::getuid().as_raw())),
                process_id: ProcessId(i64::from(std::process::id())),
            }),
            Some((peer_id.clone(), 1))
        );
        assert!(matches!(
            store.configure(CoreSet::RemovePeer(peer_id.clone()), || Ok(())),
            Output::Configured(_)
        ));
        assert_eq!(
            store.configure(CoreSet::RemovePeer(peer_id.clone()), || Ok(())),
            Output::ConfigureRefused(
                meta_signal_capsule_nexus::ConfigurationRefusal::UnknownPeer(peer_id)
            )
        );
    }

    #[test]
    fn exited_pidfd_binding_is_not_admitted() {
        use core_meta_capsule_nexus::CoreSet;
        use shared_translator::{PeerBinding, PeerId, PeerIdentity, ProcessId, UserId};
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "sleep 30"])
            .spawn()
            .expect("fixture child");
        let store = ConfigurationStore::default();
        let peer_id = PeerId("short-lived".into());
        let identity = PeerIdentity {
            user_id: UserId(i64::from(nix::unistd::getuid().as_raw())),
            process_id: ProcessId(i64::from(child.id())),
        };
        assert!(matches!(
            store.configure(CoreSet::Peer(PeerBinding {
                peer_id,
                peer_identity: identity.clone(),
            }), || Ok(())),
            core_meta_capsule_nexus::Output::Configured(_)
        ));
        child.kill().expect("terminate fixture child");
        child.wait().expect("reap fixture child");
        assert_eq!(
            store.peer_for(&shared_translator::Caller {
                user_id: identity.user_id,
                process_id: identity.process_id,
            }),
            None
        );
    }

    #[test]
    fn configured_peer_cannot_forge_the_serialized_caller() {
        use shared_translator::{PeerId, ProcessId, UserId};
        let actual = shared_translator::Caller {
            user_id: UserId(1000),
            process_id: ProcessId(42),
        };
        let forged = shared_translator::Caller {
            process_id: ProcessId(43),
            ..actual.clone()
        };
        assert_eq!(
            principal_for(
                &PublicAuthority::Peer { peer_id: PeerId("peer".into()), generation: 1 },
                &actual,
                &actual,
            ).expect("matching kernel Caller"),
            shared_translator::Principal::Peer(PeerId("peer".into()))
        );
        assert_eq!(
            principal_for(&PublicAuthority::Peer { peer_id: PeerId("peer".into()), generation: 1 }, &actual, &forged)
                .expect_err("forged Caller rejected")
                .kind(),
            std::io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn peer_rebind_closes_old_binding_subscriptions() {
        use core_meta_capsule_nexus::CoreSet;
        use shared_translator::{PeerBinding, PeerId, PeerIdentity, ProcessId, UserId};
        let store = ConfigurationStore::default();
        let peer_id = PeerId("stable".into());
        let binding = PeerBinding {
            peer_id: peer_id.clone(),
            peer_identity: PeerIdentity {
                user_id: UserId(i64::from(nix::unistd::getuid().as_raw())),
                process_id: ProcessId(i64::from(std::process::id())),
            },
        };
        assert!(matches!(store.configure(CoreSet::Peer(binding.clone()), || Ok(())), core_meta_capsule_nexus::Output::Configured(_)));
        let queue = Arc::new(SubscriptionClosing::default());
        assert!(store.register_peer_subscriber(&peer_id, 1, &queue));
        assert!(queue.is_open());
        assert!(matches!(store.configure(CoreSet::Peer(binding), || Ok(())), core_meta_capsule_nexus::Output::Configured(_)));
        assert!(!queue.is_open());
    }

    #[test]
    fn subscription_registration_rejects_a_rebound_generation() {
        use core_meta_capsule_nexus::CoreSet;
        use shared_translator::{PeerBinding, PeerId, PeerIdentity, ProcessId, UserId};
        let store = ConfigurationStore::default();
        let peer_id = PeerId("stable".into());
        let identity = PeerIdentity {
            user_id: UserId(i64::from(nix::unistd::getuid().as_raw())),
            process_id: ProcessId(i64::from(std::process::id())),
        };
        let binding = PeerBinding { peer_id: peer_id.clone(), peer_identity: identity.clone() };
        assert!(matches!(store.configure(CoreSet::Peer(binding.clone()), || Ok(())), core_meta_capsule_nexus::Output::Configured(_)));
        let caller = shared_translator::Caller {
            user_id: identity.user_id,
            process_id: identity.process_id,
        };
        let (_, old_generation) = store.peer_for(&caller).expect("first binding admitted");
        assert!(matches!(store.configure(CoreSet::Peer(binding), || Ok(())), core_meta_capsule_nexus::Output::Configured(_)));
        let stale = Arc::new(SubscriptionClosing::default());
        assert!(!store.register_peer_subscriber(&peer_id, old_generation, &stale));
        assert!(!stale.is_open());
        let (_, new_generation) = store.peer_for(&caller).expect("replacement binding admitted");
        let current = Arc::new(SubscriptionClosing::default());
        assert!(store.register_peer_subscriber(&peer_id, new_generation, &current));
        assert!(current.is_open());
    }

    #[test]
    fn rebind_cannot_cross_validated_subscription_registration() {
        use core_meta_capsule_nexus::CoreSet;
        use shared_translator::{PeerBinding, PeerId, PeerIdentity, ProcessId, UserId};
        use std::sync::{Arc, Barrier, mpsc};
        use std::time::Duration;

        let store = Arc::new(ConfigurationStore::default());
        let peer_id = PeerId("stable".into());
        let identity = PeerIdentity {
            user_id: UserId(i64::from(nix::unistd::getuid().as_raw())),
            process_id: ProcessId(i64::from(std::process::id())),
        };
        let binding = PeerBinding {
            peer_id: peer_id.clone(),
            peer_identity: identity.clone(),
        };
        assert!(matches!(
            store.configure(CoreSet::Peer(binding.clone()), || Ok(())),
            core_meta_capsule_nexus::Output::Configured(_)
        ));
        let caller = shared_translator::Caller {
            user_id: identity.user_id,
            process_id: identity.process_id,
        };
        let (_, old_generation) = store.peer_for(&caller).expect("first binding admitted");
        let stale = Arc::new(SubscriptionClosing::default());
        let validated = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));

        let registration = {
            let store = Arc::clone(&store);
            let peer_id = peer_id.clone();
            let stale = Arc::clone(&stale);
            let validated = Arc::clone(&validated);
            let release = Arc::clone(&release);
            std::thread::spawn(move || {
                store.register_peer_subscriber_after_validation(
                    &peer_id,
                    old_generation,
                    &stale,
                    || {
                        validated.wait();
                        release.wait();
                    },
                )
            })
        };
        validated.wait();

        let (rebound, observed) = mpsc::channel();
        let rebinding = {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                let response = store.configure(CoreSet::Peer(binding), || Ok(()));
                rebound.send(response).expect("report rebind");
            })
        };
        assert!(
            observed.recv_timeout(Duration::from_millis(50)).is_err(),
            "rebind must wait while validated registration holds the peer transaction"
        );
        release.wait();
        assert!(registration.join().expect("registration thread"));
        assert!(matches!(
            observed.recv_timeout(Duration::from_secs(1)).expect("rebind completed"),
            core_meta_capsule_nexus::Output::Configured(_)
        ));
        rebinding.join().expect("rebind thread");
        assert!(!stale.is_open(), "rebind closes the just-registered old generation");

        let (_, new_generation) = store.peer_for(&caller).expect("replacement binding admitted");
        let current = Arc::new(SubscriptionClosing::default());
        assert!(store.register_peer_subscriber(&peer_id, new_generation, &current));
        assert!(current.is_open(), "the current generation remains subscribed");
    }

    #[test]
    fn socket_emitter_waits_idle_then_bounds_progress_and_capacity() {
        use emission_capsule_nexus::{ExternalOutcome, Herdr, Verb};
        use nexus_exchange::lane::{Kind, Lane, LaneReading, LaneWriting};
        use signal::{ByteViewable, Signalizable};
        use std::os::unix::net::UnixListener;

        let directory = std::env::temp_dir().join(format!(
            "herdr-emitter-bounds-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir(&directory).expect("fixture directory");
        let herdr = Herdr {
            session: signal_capsule_nexus::Session("bounds".into()),
            verb: Verb::PaneList,
        };

        let timeout_socket = directory.join("timeout.sock");
        let timeout_listener = UnixListener::bind(&timeout_socket).expect("timeout listener");
        let timeout_peer = std::thread::spawn(move || {
            let (mut stream, _) = timeout_listener.accept().expect("timeout connection");
            let request = LaneReading::read_lane_frame(
                &mut stream,
                signal::MAXIMUM_SIGNAL_BYTES,
            )
            .expect("emission request");
            assert_eq!(request.lane, Lane::EmissionSignal);
            assert_eq!(request.kind, Kind::Request);
            std::thread::sleep(Duration::from_millis(100));
            let answer = emission_capsule_nexus::Output::Emitted(ExternalOutcome::Started(
                signal_capsule_nexus::State::Idle,
            ))
            .signalize()
            .expect("signalize answer");
            LaneWriting::write_lane_frame(
                &mut stream,
                &nexus_exchange::lane::Frame {
                    lane: Lane::EmissionSignal,
                    kind: Kind::Answered,
                    payload: answer.bytes().to_vec(),
                },
                signal::MAXIMUM_SIGNAL_BYTES,
            )
            .expect("emission answer");
        });
        let emitter = SocketEmitter {
            lane: EmissionLane::from(timeout_socket),
        };
        let mut configuration = (*test_configuration()).clone();
        configuration.transport_timeout = Duration::from_millis(10);
        let began = Instant::now();
        assert!(matches!(
            emitter.emit(&configuration, herdr.clone()),
            EmissionResult::External(ExternalOutcome::Started(
                signal_capsule_nexus::State::Idle
            ))
        ));
        assert!(began.elapsed() >= Duration::from_millis(90));
        timeout_peer.join().expect("timeout peer");

        let capacity_socket = directory.join("capacity.sock");
        let capacity_listener = UnixListener::bind(&capacity_socket).expect("capacity listener");
        let capacity_peer = std::thread::spawn(move || {
            let _ = capacity_listener.accept().expect("capacity connection");
        });
        let emitter = SocketEmitter {
            lane: EmissionLane::from(capacity_socket),
        };
        configuration.frame_capacity_bytes = 1;
        assert!(matches!(
            emitter.emit(&configuration, herdr),
            EmissionResult::Unavailable(_)
        ));
        capacity_peer.join().expect("capacity peer");
        std::fs::remove_dir_all(directory).expect("fixture cleanup");
    }

    #[test]
    fn four_unowned_mutations_stop_before_the_emitter() {
        use signal_capsule_nexus::{
            AgentKind, AgentName, Command, Executable, Keys, Label, Press_Data, Program,
            Prompt_Data, Input, Start_Data, Text,
        };
        let emitter = Arc::new(CountingEmitter(std::sync::atomic::AtomicUsize::new(0)));
        let dispatcher = Dispatcher::with_emitter(&test_paths(), emitter.clone());
        let pane = test_pane();
        let configuration = test_configuration();
        let command = Command {
            agent_name: AgentName("probe".into()),
            agent_kind: AgentKind::Codex,
            executable: Executable::Explicit(Program("/bin/true".into())),
            string_vector: Vec::new(),
        };
        let queries = [
            Input::Start(Start_Data {
                pane: pane.clone(),
                command,
                label: Label("label".into()),
            }),
            Input::Prompt(Prompt_Data {
                pane: pane.clone(),
                text: Text("hello".into()),
            }),
            Input::Press(Press_Data {
                pane: pane.clone(),
                keys: Keys(vec!["x".into()]),
            }),
            Input::Close(pane),
        ];
        for query in queries {
            let response = dispatcher.dispatch(query, Arc::clone(&configuration));
            let outcome = match response {
                signal_capsule_nexus::Output::Started(outcome)
                | signal_capsule_nexus::Output::Prompted(outcome)
                | signal_capsule_nexus::Output::Pressed(outcome)
                | signal_capsule_nexus::Output::Closed(outcome) => outcome,
                _ => panic!("mutation response"),
            };
            assert!(matches!(outcome, signal_capsule_nexus::Outcome::Unknown(_)));
        }
        assert_eq!(emitter.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn empty_owned_list_is_complete_without_external_observation() {
        let emitter = Arc::new(CountingEmitter(std::sync::atomic::AtomicUsize::new(0)));
        let dispatcher = Dispatcher::with_emitter(&test_paths(), emitter.clone());
        let response = dispatcher.dispatch(
            signal_capsule_nexus::Input::List(signal_capsule_nexus::Session("empty".into())),
            test_configuration(),
        );
        assert!(matches!(
            response,
            signal_capsule_nexus::Output::Listed(signal_capsule_nexus::Outcome::Listed(ref panes)) if panes.is_empty()
        ));
        assert_eq!(emitter.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    struct StaleListEmitter {
        pane: signal_capsule_nexus::Pane,
        list_started: std::sync::mpsc::Sender<()>,
        release_list: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    fn pane_listing(pane: &signal_capsule_nexus::Pane) -> emission_capsule_nexus::PaneListing {
        emission_capsule_nexus::PaneListing {
            pane: pane.clone(),
            terminal_id: emission_capsule_nexus::TerminalId("terminal".into()),
            workspace_id: emission_capsule_nexus::WorkspaceId("workspace".into()),
            tab_id: emission_capsule_nexus::TabId("tab".into()),
        }
    }

    fn agent_listing(
        pane: &signal_capsule_nexus::Pane,
        state: signal_capsule_nexus::State,
    ) -> emission_capsule_nexus::AgentListing {
        emission_capsule_nexus::AgentListing {
            pane: pane.clone(),
            terminal_id: emission_capsule_nexus::TerminalId("terminal".into()),
            workspace_id: emission_capsule_nexus::WorkspaceId("workspace".into()),
            tab_id: emission_capsule_nexus::TabId("tab".into()),
            state,
        }
    }

    impl ExternalEmitter for StaleListEmitter {
        fn emit(
            &self,
            _configuration: &ReadyCoreConfiguration,
            herdr: emission_capsule_nexus::Herdr,
        ) -> EmissionResult {
            use emission_capsule_nexus::{ExternalOutcome, Verb};
            EmissionResult::External(match herdr.verb {
                Verb::WorkspaceCreate(_) => ExternalOutcome::Created(self.pane.clone()),
                Verb::AgentStart(_) => ExternalOutcome::Started(signal_capsule_nexus::State::Busy),
                Verb::PaneList => {
                    self.list_started.send(()).expect("announce list");
                    self.release_list
                        .lock()
                        .expect("release lock")
                        .recv()
                        .expect("release list");
                    ExternalOutcome::Panes(vec![pane_listing(&self.pane)])
                }
                Verb::AgentList => ExternalOutcome::Agents(vec![agent_listing(
                    &self.pane,
                    signal_capsule_nexus::State::Open,
                )]),
                _ => ExternalOutcome::Done,
            })
        }
    }

    #[test]
    fn stale_list_finishing_after_start_preserves_started_state() {
        use signal_capsule_nexus::{
            AgentKind, AgentName, Command, Executable, Label, Name, Program, Input, Root,
            Start_Data, Value, Variable, Workspace,
        };
        let pane = signal_capsule_nexus::Pane {
            session: signal_capsule_nexus::Session("race".into()),
            pane_id: signal_capsule_nexus::PaneId("pane".into()),
        };
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let emitter = Arc::new(StaleListEmitter {
            pane: pane.clone(),
            list_started: started_tx,
            release_list: std::sync::Mutex::new(release_rx),
        });
        let dispatcher = Arc::new(Dispatcher::with_emitter(&test_paths(), emitter));
        let configuration = test_configuration();
        let created = dispatcher.dispatch(
            Input::Create(Workspace {
                session: pane.session.clone(),
                root: Root("/tmp".into()),
                variable_vector: vec![Variable {
                    name: Name("A".into()),
                    value: Value("B".into()),
                }],
            }),
            Arc::clone(&configuration),
        );
        assert!(matches!(
            created,
            signal_capsule_nexus::Output::Created(signal_capsule_nexus::Outcome::Created(_))
        ));

        let listing_dispatcher = Arc::clone(&dispatcher);
        let listing_configuration = Arc::clone(&configuration);
        let session = pane.session.clone();
        let listing = std::thread::spawn(move || {
            listing_dispatcher.dispatch(Input::List(session), listing_configuration)
        });
        started_rx
            .recv()
            .expect("list entered external observation");
        let started = dispatcher.dispatch(
            Input::Start(Start_Data {
                pane: pane.clone(),
                command: Command {
                    agent_name: AgentName("probe".into()),
                    agent_kind: AgentKind::Codex,
                    executable: Executable::Explicit(Program("/bin/true".into())),
                    string_vector: Vec::new(),
                },
                label: Label("flow".into()),
            }),
            configuration,
        );
        assert!(matches!(
            started,
            signal_capsule_nexus::Output::Started(signal_capsule_nexus::Outcome::Done)
        ));
        release_tx.send(()).expect("release stale list");
        let listed = listing.join().expect("listing thread");
        let signal_capsule_nexus::Output::Listed(signal_capsule_nexus::Outcome::Listed(known)) =
            listed
        else {
            panic!("complete list");
        };
        assert_eq!(known.len(), 1);
        assert_eq!(known[0].state, signal_capsule_nexus::State::Busy);
    }

    struct LostCreationEmitter {
        pane: signal_capsule_nexus::Pane,
        creation_id: std::sync::Mutex<Option<signal_capsule_nexus::CreationId>>,
        creates: std::sync::atomic::AtomicUsize,
        matching_marker: bool,
    }

    impl ExternalEmitter for LostCreationEmitter {
        fn emit(
            &self,
            _configuration: &ReadyCoreConfiguration,
            herdr: emission_capsule_nexus::Herdr,
        ) -> EmissionResult {
            use emission_capsule_nexus::{ExternalOutcome, Verb, WorkspaceId, WorkspaceListing};
            EmissionResult::External(match herdr.verb {
                Verb::WorkspaceCreate(data) => {
                    self.creates.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    *self.creation_id.lock().expect("creation id") = Some(data.creation_id);
                    ExternalOutcome::Uncertain
                }
                Verb::WorkspaceList => {
                    let creation_id = self.creation_id.lock().expect("creation id").clone();
                    ExternalOutcome::Workspaces(creation_id.into_iter().map(|creation_id| {
                        WorkspaceListing {
                            workspace_id: WorkspaceId("workspace".into()),
                            workspace_label: if self.matching_marker {
                                emission_capsule_nexus::managed_workspace_label(&creation_id)
                            } else {
                                emission_capsule_nexus::WorkspaceLabel("unrelated".into())
                            },
                        }
                    }).collect())
                }
                Verb::PaneList => ExternalOutcome::Panes(vec![pane_listing(&self.pane)]),
                Verb::AgentList => ExternalOutcome::Agents(Vec::new()),
                _ => ExternalOutcome::Done,
            })
        }
    }

    #[test]
    fn lost_creation_result_is_adopted_once_without_a_second_create() {
        use signal_capsule_nexus::{Name, Outcome, Input, Output, Root, Value, Variable, Workspace};
        let pane = signal_capsule_nexus::Pane {
            session: signal_capsule_nexus::Session("reconcile".into()),
            pane_id: signal_capsule_nexus::PaneId("pane".into()),
        };
        let emitter = Arc::new(LostCreationEmitter {
            pane: pane.clone(),
            creation_id: std::sync::Mutex::new(None),
            creates: std::sync::atomic::AtomicUsize::new(0),
            matching_marker: true,
        });
        let dispatcher = Dispatcher::with_emitter(&test_paths(), emitter.clone());
        let configuration = test_configuration();
        let created = dispatcher.dispatch(
            Input::Create(Workspace {
                session: pane.session.clone(),
                root: Root("/tmp/reconcile".into()),
                variable_vector: vec![Variable {
                    name: Name("A".into()),
                    value: Value("B".into()),
                }],
            }),
            Arc::clone(&configuration),
        );
        assert!(matches!(created, Output::Created(Outcome::Uncertain)));

        for _ in 0..2 {
            let listed = dispatcher.dispatch(
                Input::List(pane.session.clone()),
                Arc::clone(&configuration),
            );
            assert!(matches!(
                listed,
                Output::Listed(Outcome::Listed(ref known))
                    if known.len() == 1 && known[0].pane == pane
            ));
        }
        assert_eq!(emitter.creates.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn unrelated_workspace_marker_is_not_adopted() {
        use signal_capsule_nexus::{Outcome, Input, Output, Root, Workspace};
        let pane = signal_capsule_nexus::Pane {
            session: signal_capsule_nexus::Session("unrelated".into()),
            pane_id: signal_capsule_nexus::PaneId("pane".into()),
        };
        let emitter = Arc::new(LostCreationEmitter {
            pane: pane.clone(),
            creation_id: std::sync::Mutex::new(None),
            creates: std::sync::atomic::AtomicUsize::new(0),
            matching_marker: false,
        });
        let dispatcher = Dispatcher::with_emitter(&test_paths(), emitter.clone());
        let configuration = test_configuration();
        assert!(matches!(
            dispatcher.dispatch(
                Input::Create(Workspace {
                    session: pane.session.clone(),
                    root: Root("/tmp/unrelated".into()),
                    variable_vector: Vec::new(),
                }),
                Arc::clone(&configuration),
            ),
            Output::Created(Outcome::Uncertain)
        ));
        assert!(matches!(
            dispatcher.dispatch(Input::List(pane.session), configuration),
            Output::Listed(Outcome::Listed(ref known)) if known.is_empty()
        ));
        assert_eq!(emitter.creates.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    struct SubscriptionEmitter {
        pane: signal_capsule_nexus::Pane,
        listings:
            std::sync::Mutex<std::collections::VecDeque<emission_capsule_nexus::ExternalOutcome>>,
        list_count: std::sync::atomic::AtomicUsize,
        list_start_timeouts: std::sync::Mutex<Vec<Duration>>,
        close_outcome: std::sync::Mutex<Option<emission_capsule_nexus::ExternalOutcome>>,
    }

    impl ExternalEmitter for SubscriptionEmitter {
        fn emit(
            &self,
            configuration: &ReadyCoreConfiguration,
            herdr: emission_capsule_nexus::Herdr,
        ) -> EmissionResult {
            use emission_capsule_nexus::{ExternalOutcome, Verb};
            EmissionResult::External(match herdr.verb {
                Verb::WorkspaceCreate(_) => ExternalOutcome::Created(self.pane.clone()),
                Verb::AgentStart(_) => ExternalOutcome::Started(signal_capsule_nexus::State::Busy),
                Verb::PaneList => {
                    self.list_start_timeouts
                        .lock()
                        .expect("list configurations")
                        .push(configuration.start_timeout);
                    self.list_count
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    ExternalOutcome::Panes(vec![pane_listing(&self.pane)])
                }
                Verb::AgentList => self
                    .listings
                    .lock()
                    .expect("listings")
                    .pop_front()
                    .unwrap_or_else(|| {
                        ExternalOutcome::Agents(vec![agent_listing(
                            &self.pane,
                            signal_capsule_nexus::State::Open,
                        )])
                    }),
                Verb::PaneClose(_) => self
                    .close_outcome
                    .lock()
                    .expect("close outcome")
                    .take()
                    .unwrap_or(ExternalOutcome::Done),
                _ => ExternalOutcome::Done,
            })
        }
    }

    fn started_dispatcher(
        listings: Vec<emission_capsule_nexus::ExternalOutcome>,
    ) -> (
        Dispatcher,
        Arc<SubscriptionEmitter>,
        signal_capsule_nexus::Pane,
    ) {
        use signal_capsule_nexus::{
            AgentKind, AgentName, Command, Executable, Label, Program, Input, Root, Start_Data,
            Workspace,
        };
        let pane = signal_capsule_nexus::Pane {
            session: signal_capsule_nexus::Session("subscription".into()),
            pane_id: signal_capsule_nexus::PaneId("pane".into()),
        };
        let emitter = Arc::new(SubscriptionEmitter {
            pane: pane.clone(),
            listings: std::sync::Mutex::new(listings.into()),
            list_count: std::sync::atomic::AtomicUsize::new(0),
            list_start_timeouts: std::sync::Mutex::new(Vec::new()),
            close_outcome: std::sync::Mutex::new(None),
        });
        let dispatcher = Dispatcher::with_emitter(&test_paths(), emitter.clone());
        let configuration = test_configuration();
        let created = dispatcher.dispatch(
            Input::Create(Workspace {
                session: pane.session.clone(),
                root: Root("/tmp".into()),
                variable_vector: Vec::new(),
            }),
            Arc::clone(&configuration),
        );
        assert!(matches!(
            created,
            signal_capsule_nexus::Output::Created(signal_capsule_nexus::Outcome::Created(_))
        ));
        let started = dispatcher.dispatch(
            Input::Start(Start_Data {
                pane: pane.clone(),
                command: Command {
                    agent_name: AgentName("probe".into()),
                    agent_kind: AgentKind::Codex,
                    executable: Executable::Explicit(Program("/bin/true".into())),
                    string_vector: Vec::new(),
                },
                label: Label("flow".into()),
            }),
            configuration,
        );
        assert!(matches!(
            started,
            signal_capsule_nexus::Output::Started(signal_capsule_nexus::Outcome::Done)
        ));
        (dispatcher, emitter, pane)
    }

    fn read_subscription(
        stream: &mut std::os::unix::net::UnixStream,
    ) -> capsule_nexus_wire::public_call::Output {
        use signal::{FrameReading, Restorable, Signal};
        let body = stream
            .read_frame(signal::FrameCapacity::from(4096))
            .expect("subscription frame");
        match Signal::<signal::Delivery<capsule_nexus_wire::public_call::Output>>::from(Vec::from(body))
            .restore()
            .expect("typed delivery")
        {
            signal::Delivery::Answer(answer) if answer.exchange == signal::FIRST_EXCHANGE => answer.response,
            other => panic!("a subscription answers on its exchange, got {other:?}"),
        }
    }

    #[test]
    fn ownership_is_partitioned_by_principal() {
        use signal_capsule_nexus::{Outcome, Input, Output};
        let (dispatcher, emitter, pane) = started_dispatcher(Vec::new());
        let other = shared_translator::Principal::Peer(shared_translator::PeerId("other".into()));
        assert_eq!(
            dispatcher.dispatch_as(
                other.clone(),
                Input::List(pane.session.clone()),
                test_configuration(),
            ).answered().expect("List is answered"),
            Output::Listed(Outcome::Listed(Vec::new()))
        );
        let before = emitter.list_count.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            dispatcher.dispatch_as(other, Input::Close(pane.clone()), test_configuration())
                .answered().expect("Close is answered"),
            Output::Closed(Outcome::Unknown(pane))
        );
        assert_eq!(
            emitter.list_count.load(std::sync::atomic::Ordering::SeqCst),
            before,
            "foreign ownership must stop before external observation"
        );
    }

    #[test]
    fn subscription_opens_on_the_live_panes_then_tells_one_harness_loss() {
        use emission_capsule_nexus::ExternalOutcome;
        use signal_capsule_nexus::State;
        let (dispatcher, _emitter, pane) = started_dispatcher(vec![
            ExternalOutcome::Uncertain,
            ExternalOutcome::Agents(vec![agent_listing(&pane_for("subscription", "pane"), State::Open)]),
        ]);
        let mut client = dispatcher.subscribe(pane.session.clone(), test_configuration());
        client
            .set_read_timeout(Some(Duration::from_millis(250)))
            .expect("timeout");
        assert_eq!(read_frame(&mut client), opened(vec![known(&pane, Some("flow"), State::Busy)]));
        assert_eq!(read_frame(&mut client), changed(&pane, State::Dead));
        assert_quiet(&mut client, 35, "repeated loss must not tell Dead twice");
    }

    fn pane_for(session: &str, pane: &str) -> signal_capsule_nexus::Pane {
        signal_capsule_nexus::Pane {
            session: signal_capsule_nexus::Session(session.into()),
            pane_id: signal_capsule_nexus::PaneId(pane.into()),
        }
    }

    type Frame = signal::Frame<signal_capsule_nexus::Panes, signal_capsule_nexus::Known>;

    fn known(
        pane: &signal_capsule_nexus::Pane,
        label: Option<&str>,
        state: signal_capsule_nexus::State,
    ) -> signal_capsule_nexus::Known {
        signal_capsule_nexus::Known {
            pane: pane.clone(),
            label_option: label.map(|label| signal_capsule_nexus::Label(label.into())),
            state,
            attempt: signal_capsule_nexus::Attempt::Settled,
        }
    }

    fn opened(panes: Vec<signal_capsule_nexus::Known>) -> Frame {
        signal::Frame::Opened(signal_capsule_nexus::Panes(panes))
    }

    fn changed(pane: &signal_capsule_nexus::Pane, state: signal_capsule_nexus::State) -> Frame {
        signal::Frame::Changed(known(pane, Some("flow"), state))
    }

    /// The next frame of the subscription on the first exchange.
    fn read_frame(stream: &mut std::os::unix::net::UnixStream) -> Frame {
        match read_subscription(stream) {
            capsule_nexus_wire::public_call::Output::Answered(output) => match output.0 {
                signal_capsule_nexus::Output::Subscribed(frame) => frame,
                other => panic!("a subscription answers with its frames, got {other:?}"),
            },
            other => panic!("a subscription is answered, got {other:?}"),
        }
    }

    fn assert_quiet(stream: &mut std::os::unix::net::UnixStream, milliseconds: u64, why: &str) {
        use signal::FrameReading;
        stream
            .set_read_timeout(Some(Duration::from_millis(milliseconds)))
            .expect("short timeout");
        assert!(stream.read_frame(signal::FrameCapacity::from(4096)).is_err(), "{why}");
    }

    #[test]
    fn two_subscribers_share_one_session_poll_source() {
        let (dispatcher, emitter, pane) = started_dispatcher(Vec::new());
        let baseline = emitter.list_count.load(std::sync::atomic::Ordering::SeqCst);
        let started = std::time::Instant::now();
        let mut first = dispatcher.subscribe(pane.session.clone(), test_configuration());
        let mut second = dispatcher.subscribe(pane.session.clone(), test_configuration());
        first.set_read_timeout(Some(Duration::from_millis(250))).expect("timeout");
        second.set_read_timeout(Some(Duration::from_millis(250))).expect("timeout");
        let _ = read_frame(&mut first);
        let _ = read_frame(&mut second);
        assert_eq!(dispatcher.subscription_source_count(), 1);
        // Readiness: wait until the shared source has polled enough times to
        // show its cadence, within a deadline.
        let wanted = baseline + 6;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut polls = emitter.list_count.load(std::sync::atomic::Ordering::SeqCst);
        while polls < wanted && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
            polls = emitter.list_count.load(std::sync::atomic::Ordering::SeqCst);
        }
        let elapsed = started.elapsed();
        assert!(polls >= wanted, "shared source did not poll, got {polls}");
        // One source polls once per interval; two independent sources would
        // poll about twice as often. The bound scales with the time actually
        // elapsed, so load only loosens it.
        let interval = test_configuration().polling;
        let allowed = (elapsed.as_millis() / interval.as_millis()) as usize + 2;
        assert!(
            polls - baseline <= allowed,
            "one shared source should poll at one cadence, got {} in {elapsed:?}",
            polls - baseline
        );
    }

    #[test]
    fn quiet_disconnected_subscriber_releases_its_shared_poll_source() {
        let (dispatcher, emitter, pane) = started_dispatcher(Vec::new());
        let mut client = dispatcher.subscribe(pane.session.clone(), test_configuration());
        client.set_read_timeout(Some(Duration::from_millis(250))).expect("timeout");
        let _ = read_frame(&mut client);
        assert_eq!(dispatcher.subscription_source_count(), 1);
        drop(client);
        let deadline = std::time::Instant::now() + Duration::from_millis(250);
        while dispatcher.subscription_source_count() != 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(dispatcher.subscription_source_count(), 0, "the last stream takes the Publisher");
        // The feed ends with the Publisher: no observation follows it.
        std::thread::sleep(test_configuration().polling * 3);
        let after = emitter.list_count.load(std::sync::atomic::Ordering::SeqCst);
        std::thread::sleep(test_configuration().polling * 5);
        assert_eq!(
            emitter.list_count.load(std::sync::atomic::Ordering::SeqCst),
            after,
            "no feed observes a session without a subscriber"
        );
    }

    #[test]
    fn each_change_arrives_once_and_in_order() {
        use emission_capsule_nexus::ExternalOutcome;
        use signal_capsule_nexus::State;
        let pane = pane_for("subscription", "pane");
        let (dispatcher, _emitter, _) = started_dispatcher(vec![
            ExternalOutcome::Agents(vec![agent_listing(&pane, State::Idle)]),
            ExternalOutcome::Agents(vec![agent_listing(&pane, State::Idle)]),
            ExternalOutcome::Agents(vec![agent_listing(&pane, State::Busy)]),
            ExternalOutcome::Agents(vec![agent_listing(&pane, State::Busy)]),
            ExternalOutcome::Agents(vec![agent_listing(&pane, State::Idle)]),
        ]);
        let mut client = dispatcher.subscribe(pane.session.clone(), test_configuration());
        client.set_read_timeout(Some(Duration::from_millis(250))).expect("timeout");
        assert_eq!(read_frame(&mut client), opened(vec![known(&pane, Some("flow"), State::Busy)]));
        for state in [State::Idle, State::Busy, State::Idle, State::Dead] {
            assert_eq!(read_frame(&mut client), changed(&pane, state));
        }
        assert_quiet(&mut client, 35, "an unchanged observation tells nothing");
    }

    #[test]
    fn a_non_owner_may_subscribe_and_observes_every_pane_of_the_session() {
        use signal_capsule_nexus::{Input, Outcome, Output, State};
        let (dispatcher, _emitter, pane) = started_dispatcher(Vec::new());
        let stranger = shared_translator::UserId(i64::from(nix::unistd::getuid().as_raw()) + 4242);
        let mut client = dispatcher.subscribe_as(
            PublicAuthority::Translator,
            stranger,
            pane.session.clone(),
            test_configuration(),
        );
        client.set_read_timeout(Some(Duration::from_millis(250))).expect("timeout");
        assert_eq!(read_frame(&mut client), opened(vec![known(&pane, Some("flow"), State::Busy)]));
        assert_eq!(
            dispatcher
                .dispatch_as(
                    shared_translator::Principal::Login(stranger),
                    Input::Close(pane.clone()),
                    test_configuration(),
                )
                .answered()
                .expect("Close is answered"),
            Output::Closed(Outcome::Unknown(pane)),
            "acting stays the owner's"
        );
    }

    #[test]
    fn late_subscriber_cursor_does_not_repeat_its_current_state() {
        use emission_capsule_nexus::ExternalOutcome;
        use signal_capsule_nexus::{AgentKind, AgentName, Command, Executable, Label, Program, Input, Start_Data, State};
        let pane = pane_for("subscription", "pane");
        let (dispatcher, _emitter, _) = started_dispatcher(vec![
            ExternalOutcome::Agents(vec![agent_listing(&pane, State::Idle)]),
            ExternalOutcome::Agents(vec![agent_listing(&pane, State::Busy)]),
        ]);
        let slow = Arc::new(ReadyCoreConfiguration {
            polling: Duration::from_millis(100),
            start_timeout: Duration::from_millis(20),
            frame_capacity_bytes: 4096,
            transport_timeout: Duration::from_millis(30),
        });
        *dispatcher.configuration.current.write().expect("configuration") = Some(Arc::clone(&slow));
        let mut client_a = dispatcher.subscribe(pane.session.clone(), Arc::clone(&slow));
        client_a.set_read_timeout(Some(Duration::from_millis(300))).expect("timeout a");
        assert_eq!(read_frame(&mut client_a), opened(vec![known(&pane, Some("flow"), State::Busy)]));
        assert_eq!(read_frame(&mut client_a), changed(&pane, State::Idle));

        let started = dispatcher.dispatch(Input::Start(Start_Data {
            pane: pane.clone(),
            command: Command {
                agent_name: AgentName("probe".into()),
                agent_kind: AgentKind::Codex,
                executable: Executable::Explicit(Program("/bin/true".into())),
                string_vector: Vec::new(),
            },
            label: Label("flow".into()),
        }), Arc::clone(&slow));
        assert!(matches!(started, signal_capsule_nexus::Output::Started(signal_capsule_nexus::Outcome::Done)));
        let mut client_b = dispatcher.subscribe(pane.session.clone(), Arc::clone(&slow));
        client_b.set_read_timeout(Some(Duration::from_millis(300))).expect("timeout b");
        assert_eq!(read_frame(&mut client_b), opened(vec![known(&pane, Some("flow"), State::Busy)]));
        assert_eq!(read_frame(&mut client_a), changed(&pane, State::Busy));
        assert_quiet(&mut client_b, 35, "late subscriber must not receive its current Busy twice");
    }

    #[test]
    fn a_subscribe_configured_before_a_resize_opens_at_the_new_capacity() {
        use futures_util::StreamExt;
        use signal_capsule_nexus::State;
        let (dispatcher, _emitter, pane) = started_dispatcher(Vec::new());
        // The fixture configured capacity 8; a Subscribe whose caller read
        // its configuration then still opens at what the actor holds now.
        assert!(matches!(
            dispatcher.configuration.configure(core_meta_capsule_nexus::CoreSet::SubscriberCapacity(1), || Ok(())),
            core_meta_capsule_nexus::Output::Configured(_)
        ));
        let session = pane.session.clone();
        let subscription = settled(dispatcher.capsule.ask(Subscribe { session: session.clone() }).blocking_send())
            .expect("subscribes");
        let mut stream = subscription.stream;
        for state in [State::Idle, State::Busy] {
            let (captured, pending, _) =
                settled(dispatcher.capsule.ask(CaptureListing { session: session.clone() }).blocking_send())
                    .expect("captures");
            dispatcher
                .capsule
                .ask(FinishObservation {
                    session: session.clone(),
                    captured,
                    pending,
                    adoption: None,
                    external: capsule_nexus_core::ExternalResult::Succeeded(vec![
                        emission_capsule_nexus::PaneObservation { pane: pane.clone(), state },
                    ]),
                })
                .blocking_send()
                .expect("observes");
        }
        let (opened, second) = test_runtime().block_on(async { (stream.next().await, stream.next().await) });
        assert!(matches!(opened, Some(signal::Frame::Opened(_))));
        assert!(
            matches!(second, Some(signal::Frame::Lagged(_))),
            "two changes past a capacity of one lag the stream, got {second:?} instead of the old capacity's Changed"
        );
    }

    #[test]
    fn active_subscription_reads_replaced_configuration() {
        let (dispatcher, emitter, pane) = started_dispatcher(Vec::new());
        let _client = dispatcher.subscribe(pane.session, test_configuration());
        while emitter.list_count.load(std::sync::atomic::Ordering::SeqCst) < 1 {
            std::thread::sleep(Duration::from_millis(2));
        }
        let replacement = Arc::new(ReadyCoreConfiguration {
            polling: Duration::from_millis(10),
            start_timeout: Duration::from_millis(25),
            frame_capacity_bytes: 4096,
            transport_timeout: Duration::from_millis(40),
        });
        *dispatcher.configuration.current.write().expect("configuration") = Some(replacement);
        let deadline = Instant::now() + Duration::from_millis(300);
        while !emitter.list_start_timeouts.lock().expect("configurations")
            .iter().any(|timeout| *timeout == Duration::from_millis(25))
        {
            assert!(Instant::now() < deadline, "subscription did not adopt replaced configuration");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn successful_close_tells_one_dead_and_a_later_opened_holds_no_dead_pane() {
        use signal_capsule_nexus::{Outcome, Input, Output, State};
        let (dispatcher, _emitter, pane) = started_dispatcher(Vec::new());
        let configuration = test_configuration();
        let mut client = dispatcher.subscribe(pane.session.clone(), Arc::clone(&configuration));
        client.set_read_timeout(Some(Duration::from_millis(150))).expect("timeout");
        assert_eq!(read_frame(&mut client), opened(vec![known(&pane, Some("flow"), State::Busy)]));
        assert!(matches!(
            dispatcher.dispatch(Input::Close(pane.clone()), Arc::clone(&configuration)),
            Output::Closed(Outcome::Done)
        ));
        assert_eq!(read_frame(&mut client), changed(&pane, State::Dead));
        assert_quiet(&mut client, 40, "Dead is told once");

        let mut late = dispatcher.subscribe(pane.session, configuration);
        late.set_read_timeout(Some(Duration::from_millis(150))).expect("late timeout");
        assert_eq!(read_frame(&mut late), opened(Vec::new()));
        assert_quiet(&mut late, 40, "a late subscriber is told nothing of a closed pane");
    }

    #[test]
    fn externally_absent_owned_pane_is_released_and_publishes_one_dead() {
        use emission_capsule_nexus::ExternalOutcome;
        use signal_capsule_nexus::{
            ErrorCode, Message, Outcome, Input, Refusal, Output, State,
        };
        let (dispatcher, emitter, pane) = started_dispatcher(Vec::new());
        *emitter.close_outcome.lock().expect("close outcome") = Some(
            ExternalOutcome::Refused(Refusal {
                error_code: ErrorCode("pane_not_found".into()),
                message: Message("pane is already absent".into()),
                exit_status_option: None,
            }),
        );
        let configuration = test_configuration();
        let mut client = dispatcher.subscribe(pane.session.clone(), Arc::clone(&configuration));
        client
            .set_read_timeout(Some(Duration::from_millis(150)))
            .expect("timeout");
        let _ = read_frame(&mut client);

        assert!(matches!(
            dispatcher.dispatch(Input::Close(pane.clone()), Arc::clone(&configuration)),
            Output::Closed(Outcome::Done)
        ));
        assert_eq!(read_frame(&mut client), changed(&pane, State::Dead));
        assert!(matches!(
            dispatcher.dispatch(Input::List(pane.session), configuration),
            Output::Listed(Outcome::Listed(ref known)) if known.is_empty()
        ));
    }

    #[test]
    fn another_close_refusal_retains_ownership() {
        use emission_capsule_nexus::ExternalOutcome;
        use signal_capsule_nexus::{ErrorCode, Message, Outcome, Input, Refusal, Output};
        let (dispatcher, emitter, pane) = started_dispatcher(Vec::new());
        *emitter.close_outcome.lock().expect("close outcome") = Some(
            ExternalOutcome::Refused(Refusal {
                error_code: ErrorCode("workspace_busy".into()),
                message: Message("still present".into()),
                exit_status_option: None,
            }),
        );
        let configuration = test_configuration();
        let mut client = dispatcher.subscribe(pane.session.clone(), Arc::clone(&configuration));
        client
            .set_read_timeout(Some(Duration::from_millis(80)))
            .expect("timeout");
        let _ = read_frame(&mut client);

        assert!(matches!(
            dispatcher.dispatch(Input::Close(pane.clone()), Arc::clone(&configuration)),
            Output::Closed(Outcome::Refused(ref refusal)) if refusal.error_code.0 == "workspace_busy"
        ));
        assert!(matches!(
            dispatcher.dispatch(Input::List(pane.session), configuration),
            Output::Listed(Outcome::Listed(ref known)) if known.len() == 1
        ));
    }

    #[test]
    fn stale_poll_finishing_after_close_cannot_reopen_the_pane() {
        use signal_capsule_nexus::{
            AgentKind, AgentName, Command, Executable, Label, Outcome, Program, Input, Output,
            Root, Start_Data, State, Workspace,
        };
        let pane = pane_for("stale-close", "pane");
        let (poll_started_tx, poll_started_rx) = std::sync::mpsc::channel();
        let (release_poll_tx, release_poll_rx) = std::sync::mpsc::channel();
        let emitter = Arc::new(StaleListEmitter {
            pane: pane.clone(),
            list_started: poll_started_tx,
            release_list: std::sync::Mutex::new(release_poll_rx),
        });
        let dispatcher = Dispatcher::with_emitter(&test_paths(), emitter);
        let configuration = test_configuration();
        assert!(matches!(
            dispatcher.dispatch(
                Input::Create(Workspace {
                    session: pane.session.clone(),
                    root: Root("/tmp".into()),
                    variable_vector: Vec::new(),
                }),
                Arc::clone(&configuration),
            ),
            Output::Created(Outcome::Created(_))
        ));
        assert!(matches!(
            dispatcher.dispatch(
                Input::Start(Start_Data {
                    pane: pane.clone(),
                    command: Command {
                        agent_name: AgentName("probe".into()),
                        agent_kind: AgentKind::Codex,
                        executable: Executable::Explicit(Program("/bin/true".into())),
                        string_vector: Vec::new(),
                    },
                    label: Label("flow".into()),
                }),
                Arc::clone(&configuration),
            ),
            Output::Started(Outcome::Done)
        ));

        let mut client = dispatcher.subscribe(pane.session.clone(), Arc::clone(&configuration));
        client.set_read_timeout(Some(Duration::from_millis(250))).expect("timeout");
        assert_eq!(read_frame(&mut client), opened(vec![known(&pane, Some("flow"), State::Busy)]));
        poll_started_rx.recv().expect("old poll reached external List");

        assert!(matches!(
            dispatcher.dispatch(Input::Close(pane.clone()), Arc::clone(&configuration)),
            Output::Closed(Outcome::Done)
        ));
        assert_eq!(read_frame(&mut client), changed(&pane, State::Dead));
        release_poll_tx.send(()).expect("release stale poll");
        assert_quiet(&mut client, 80, "the stale pre-Close List must not publish Open after terminal Dead");
    }

    #[test]
    fn a_peer_rebind_withdraws_its_subscription_and_closes_its_connection() {
        use core_meta_capsule_nexus::CoreSet;
        use shared_translator::{PeerBinding, PeerId, PeerIdentity, ProcessId, UserId};
        let (dispatcher, _emitter, pane) = started_dispatcher(Vec::new());
        let identity = PeerIdentity {
            user_id: UserId(i64::from(nix::unistd::getuid().as_raw())),
            process_id: ProcessId(i64::from(std::process::id())),
        };
        let binding = PeerBinding {
            peer_id: PeerId("observer".into()),
            peer_identity: identity.clone(),
        };
        let configure = |binding: PeerBinding| {
            assert!(matches!(
                dispatcher.configuration.configure(CoreSet::Peer(binding), || Ok(())),
                core_meta_capsule_nexus::Output::Configured(_)
            ));
        };
        configure(binding.clone());
        let (peer_id, generation) = dispatcher
            .configuration
            .peer_for(&shared_translator::Caller {
                user_id: identity.user_id,
                process_id: identity.process_id,
            })
            .expect("the peer is bound");
        let mut client = dispatcher.subscribe_as(
            PublicAuthority::Peer { peer_id, generation },
            identity.user_id,
            pane.session.clone(),
            test_configuration(),
        );
        client.set_read_timeout(Some(Duration::from_millis(500))).expect("timeout");
        let _ = read_frame(&mut client);
        configure(binding);
        let capacity = signal::FrameCapacity::from(4096);
        assert!(
            nexus_exchange::EndingFrameReading::read_frame_or_end(&mut client, capacity)
                .expect("the end of the connection")
                .is_none(),
            "a withdrawn subscription closes its connection"
        );
    }

    fn public_query(
        session: &str,
        caller: &shared_translator::Caller,
    ) -> capsule_nexus_wire::public_call::Input {
        capsule_nexus_wire::public_call::Input::Call(capsule_nexus_wire::public_call::Call_Data {
            caller: caller.clone(),
            public_input: capsule_nexus_wire::public_payload::Input(
                signal_capsule_nexus::Input::List(signal_capsule_nexus::Session(session.into())),
            ),
        })
    }

    #[test]
    fn a_public_connection_greets_once_then_answers_each_exchange_on_its_identifier() {
        use nexus_querying::{Patience, Querying, QueryingConnection};
        use capsule_nexus_wire::public_call::{Input, Output};
        use signal::Delivery;
        let (dispatcher, _emitter, pane) = started_dispatcher(Vec::new());
        let (server, client) = std::os::unix::net::UnixStream::pair().expect("pair");
        let caller = capsule_nexus_wire::frame::peer_caller(&client).expect("caller");
        test_runtime().spawn(PublicConnection::serve(
            server,
            PublicAuthority::Translator,
            dispatcher.clone(),
            test_configuration(),
        ));
        let mut querying = QueryingConnection::<Input, Output>::greeted(
            client,
            Patience::Progress(Duration::from_millis(500)),
            signal::FrameCapacity::from(4096),
        )
        .expect("greeted");
        let first = querying.open(public_query(&pane.session.0, &caller)).expect("first open");
        let second = querying.open(public_query("elsewhere", &caller)).expect("second open");
        assert_ne!(first, second);
        let mut answered = Vec::new();
        for _ in 0..2 {
            match querying.delivery().expect("delivery") {
                Some(Delivery::Answer(answer)) => answered.push((answer.exchange, answer.response)),
                other => panic!("a one-answer exchange is answered, got {other:?}"),
            }
        }
        answered.sort_by_key(|(exchange, _)| *exchange);
        assert!(matches!(
            &answered[0],
            (exchange, Output::Answered(output))
                if *exchange == first && matches!(&output.0, signal_capsule_nexus::Output::Listed(signal_capsule_nexus::Outcome::Listed(known)) if known.len() == 1)
        ));
        assert!(matches!(
            &answered[1],
            (exchange, Output::Answered(output))
                if *exchange == second && matches!(&output.0, signal_capsule_nexus::Output::Listed(signal_capsule_nexus::Outcome::Listed(known)) if known.is_empty())
        ));
    }

    #[test]
    fn an_exchange_opened_before_the_greeting_ends_the_connection() {
        use signal::{Delivery, Dispatch, Ending, ExchangeFault, Opening};
        let (dispatcher, _emitter, _pane) = started_dispatcher(Vec::new());
        let (server, client) = std::os::unix::net::UnixStream::pair().expect("pair");
        let caller = capsule_nexus_wire::frame::peer_caller(&client).expect("caller");
        test_runtime().spawn(PublicConnection::serve(
            server,
            PublicAuthority::Translator,
            dispatcher.clone(),
            test_configuration(),
        ));
        let capacity = signal::FrameCapacity::from(4096);
        nexus_exchange::ValueWriting::write_value(
            &client,
            &Dispatch::Open(Opening { exchange: signal::FIRST_EXCHANGE, query: public_query("any", &caller) }),
            Duration::from_millis(500),
            capacity,
        )
        .expect("open");
        let mut reading = client.try_clone().expect("reading");
        reading.set_read_timeout(Some(Duration::from_millis(500))).expect("timeout");
        let body = nexus_exchange::EndingFrameReading::read_frame_or_end(&mut reading, capacity)
            .expect("frame")
            .expect("an ending");
        assert_eq!(
            nexus_exchange::ValueRestoring::restored::<Delivery<capsule_nexus_wire::public_call::Output>>(body)
                .expect("delivery"),
            Delivery::End(Ending::connection_faulted(ExchangeFault::GreetingExpected))
        );
        assert!(nexus_exchange::EndingFrameReading::read_frame_or_end(&mut reading, capacity)
            .expect("the end of the connection")
            .is_none());
    }

}
    #[test]
    fn core_accepts_only_the_paired_translator_process() {
        use nexus_serving::{PairedPeer, PeerMatching};
        use shared_translator::{PeerIdentity, ProcessId, UserId};
        let (client, _server) = std::os::unix::net::UnixStream::pair().expect("socket pair");
        let actual = capsule_nexus_wire::frame::peer_caller(&client).expect("peer credentials");
        let paired = PairedPeer::capture(PeerIdentity {
            user_id: actual.user_id,
            process_id: actual.process_id,
        }).expect("capture current process");
        assert!(paired.require_peer(&client).is_ok());
        let forged = PairedPeer::capture(PeerIdentity {
            user_id: UserId(actual.user_id.0),
            process_id: ProcessId(actual.process_id.0),
        }).expect("capture current process");
        let mismatched = shared_translator::Caller {
            process_id: ProcessId(actual.process_id.0 + 1),
            ..actual
        };
        assert!(!forged.matches(&mismatched));
    }
