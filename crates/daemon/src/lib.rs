//! Daemon configuration lifecycle, independent of socket serving.

use meta_signal_capsule_nexus::{BootstrapRefusal, Message, SocketPath};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

pub use capsule_nexus_wire::deadline::DeadlineStream;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SocketPaths {
    pub public: std::path::PathBuf,
    pub meta: std::path::PathBuf,
    pub translator: std::path::PathBuf,
    pub meta_translator: std::path::PathBuf,
}

impl From<&shared_translator::placement::Place> for SocketPaths {
    fn from(place: &shared_translator::placement::Place) -> Self {
        let directory = shared_translator::placement::runtime_path(place);
        let names = capsule_nexus_wire::socket_names();
        Self {
            public: directory.join(names.public_nexus_socket.0),
            meta: directory.join(names.meta_nexus_socket.0),
            translator: directory.join(names.public_translator_socket.0),
            meta_translator: directory.join(names.meta_translator_socket.0),
        }
    }
}

struct OwnedListener {
    listener: std::os::unix::net::UnixListener,
    path: std::path::PathBuf,
    device: u64,
    inode: u64,
}

impl OwnedListener {
    fn bind(path: std::path::PathBuf) -> std::io::Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let listener = std::os::unix::net::UnixListener::bind(&path)?;
        let metadata = std::fs::metadata(&path)?;
        Ok(Self {
            listener,
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

impl Drop for OwnedListener {
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;
        let still_ours = std::fs::metadata(&self.path)
            .map(|metadata| metadata.dev() == self.device && metadata.ino() == self.inode)
            .unwrap_or(false);
        if still_ours {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

pub struct Bootstrap {
    paths: SocketPaths,
    store: Arc<ConfigurationStore>,
    meta: OwnedListener,
    public: std::sync::Mutex<Option<OwnedListener>>,
    public_ready: std::sync::Condvar,
    admission: capsule_nexus_wire::ValidAdmission,
    translator_peer: BoundPeer,
}

impl Bootstrap {
    pub fn bind(
        place: &shared_translator::placement::Place,
        admission: capsule_nexus_wire::ValidAdmission,
        translator_peer: shared_translator::PeerIdentity,
    ) -> std::io::Result<Self> {
        let paths = SocketPaths::from(place);
        let meta = OwnedListener::bind(paths.meta.clone())?;
        Ok(Self {
            paths,
            store: Arc::new(ConfigurationStore::default()),
            meta,
            public: std::sync::Mutex::new(None),
            public_ready: std::sync::Condvar::new(),
            admission,
            translator_peer: BoundPeer::capture(translator_peer)?,
        })
    }

    pub fn configure(
        &self,
        setting: core_meta_capsule_nexus::CoreSet,
    ) -> core_meta_capsule_nexus::Output {
        let mut staged = None;
        let response = self.store.configure(setting, || {
            let listener = OwnedListener::bind(self.paths.public.clone()).map_err(|error| {
                BootstrapRefusal {
                    socket_path: SocketPath(self.paths.public.display().to_string()),
                    message: Message(error.to_string()),
                }
            })?;
            staged = Some(listener);
            Ok(())
        });
        if let Some(listener) = staged {
            let mut public = self
                .public
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *public = Some(listener);
            self.public_ready.notify_all();
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
        let (stream, _) = capsule_nexus_wire::accept_with_retry(
            &self.meta.listener,
            self.admission.retry_delay,
        )?;
        let exchange = (|| -> Result<(), Box<dyn std::error::Error>> {
            require_peer(&stream, &self.translator_peer)?;
            let mut stream = DeadlineStream::new(stream, self.admission.deadline)?;
            serve_meta_connection(
                &mut stream,
                signal::FrameCapacity::from(self.admission.frame_capacity),
                |candidate| self.configure(candidate),
            )
        })();
        if let Err(error) = exchange {
            eprintln!("Capsule Nexus meta connection failed: {error}");
        }
        Ok(())
    }

    pub fn accept_public(&self) -> std::io::Result<AcceptedPublic> {
        let mut public = self
            .public
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while public.is_none() {
            public = self
                .public_ready
                .wait(public)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        loop {
            let stream = capsule_nexus_wire::accept_with_retry(
                &public
                .as_ref()
                .expect("public listener notified only after binding")
                .listener,
                self.admission.retry_delay,
            )?.0;
            let actual = match capsule_nexus_wire::frame::peer_caller(&stream) {
                Ok(actual) => actual,
                Err(error) => {
                    eprintln!("Capsule Nexus public peer lookup failed: {error}");
                    continue;
                }
            };
            if self.translator_peer.matches(&actual) {
                return Ok(AcceptedPublic { stream, authority: PublicAuthority::Translator });
            }
            match self.store.peer_for(&actual) {
                Some((peer_id, generation)) => return Ok(AcceptedPublic {
                    stream,
                    authority: PublicAuthority::Peer { peer_id, generation },
                }),
                None => eprintln!("Capsule Nexus rejected unconfigured public peer"),
            }
        }
    }

    pub fn paths(&self) -> &SocketPaths {
        &self.paths
    }

    pub fn admission(&self) -> capsule_nexus_wire::ValidAdmission {
        self.admission
    }
}

pub struct AcceptedPublic {
    pub stream: std::os::unix::net::UnixStream,
    pub authority: PublicAuthority,
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

fn require_peer(
    stream: &std::os::unix::net::UnixStream,
    expected: &BoundPeer,
) -> std::io::Result<()> {
    let actual = capsule_nexus_wire::frame::peer_caller(stream)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    if expected.matches(&actual) {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "peer is not the paired translator",
        ))
    }
}

/// Serves one meta connection, the whole of it within the deadline its stream
/// carries: the greeting, then each exchange in turn, each answered with its
/// one Answer, until the translator closes the connection. The caller
/// supplies the startup capacity; no runtime default is hidden here.
pub fn serve_meta_connection<S>(
    stream: &mut S,
    capacity: signal::FrameCapacity,
    mut configure: impl FnMut(core_meta_capsule_nexus::CoreSet) -> core_meta_capsule_nexus::Output,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: std::io::Read + std::io::Write,
{
    use capsule_nexus_wire::exchange::{read_frame_or_end, restored};
    use capsule_nexus_wire::meta_call::{Input, Output};
    use signal::{
        Answer, Contracted, Delivery, Dispatch, Ending, ExchangeFault, ExchangeLedger,
        ExchangeTracking, Exchanged, FrameWriting, Greeted, HandshakeReceipt, Signalizable,
    };
    let mut ledger = ExchangeLedger::default();
    let deliver = |stream: &mut S, delivery: Delivery<Output>| -> Result<(), Box<dyn std::error::Error>> {
        stream.write_frame(&delivery.signalize()?, capacity)?;
        Ok(())
    };
    while let Some(body) = read_frame_or_end(stream, capacity)? {
        let Ok(dispatch) = restored::<Dispatch<Input>>(body) else {
            return deliver(stream, Delivery::End(Ending::connection_faulted(ExchangeFault::UnreadableQuery)));
        };
        match dispatch {
            Dispatch::Greet(greeting) => {
                if let Err(fault) = ledger.greet() {
                    return deliver(stream, Delivery::End(Ending::connection_faulted(fault)));
                }
                let receipt = Input::receipt(&greeting);
                let settled = matches!(receipt, HandshakeReceipt::Greeted(_));
                deliver(stream, Delivery::Greeted(receipt))?;
                if !settled {
                    return Ok(());
                }
            }
            Dispatch::Open(opening) => {
                if let Err(fault) = ledger.require_greeting() {
                    return deliver(stream, Delivery::End(Ending::connection_faulted(fault)));
                }
                if let Err(fault) = ledger.admit(opening.exchange) {
                    let ending = Ending::faulted(opening.exchange, fault);
                    let connection_wide = ending.is_connection_wide();
                    deliver(stream, Delivery::End(ending))?;
                    if connection_wide {
                        return Ok(());
                    }
                    continue;
                }
                let Input::Call(call) = opening.query;
                let response = match call.meta_input.0 {
                    core_meta_capsule_nexus::Input::Configure(setting) => configure(setting),
                };
                ledger
                    .release(opening.exchange)
                    .map_err(|fault| format!("meta exchange release failed: {fault:?}"))?;
                deliver(stream, Delivery::Answer(Answer {
                    exchange: opening.exchange,
                    response: Output::Answered(capsule_nexus_wire::meta_payload::Output(response)),
                }))?;
            }
            Dispatch::Abandon(exchange) => {
                // Every exchange is answered before the next frame is read,
                // so none is open for the translator to abandon.
                let fault = ledger.release(exchange).err().unwrap_or(ExchangeFault::UnknownExchange);
                deliver(stream, Delivery::End(Ending::faulted(exchange, fault)))?;
            }
        }
    }
    Ok(())
}

/// One accepted public connection: one greeting, then any number of
/// exchanges, each opened with a Call carrying its Caller.
///
/// The authority is the connection's, taken from its peer credentials at
/// accept. Each exchange is answered on a thread of its own, so a slow
/// external attempt holds no other exchange back; a Subscribe goes on
/// answering on its exchange from its subscription's writer. Every frame is
/// written whole, one at a time, within the configured transport timeout.
pub struct PublicConnection {
    stream: std::os::unix::net::UnixStream,
    writing: std::sync::Mutex<std::os::unix::net::UnixStream>,
    ledger: std::sync::Mutex<signal::ExchangeLedger>,
    /// The subscriptions open on this connection, or `None` once the
    /// connection has ended.
    subscriptions: std::sync::Mutex<Option<Vec<(signal::ExchangeId, Arc<SubscriberQueue>)>>>,
    authority: PublicAuthority,
}

/// Whether a connection goes on after the frame it just handled.
enum Continuation {
    Going,
    Ended,
}

impl PublicConnection {
    pub fn new(
        stream: std::os::unix::net::UnixStream,
        authority: PublicAuthority,
    ) -> std::io::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            writing: std::sync::Mutex::new(stream.try_clone()?),
            stream,
            ledger: std::sync::Mutex::new(signal::ExchangeLedger::default()),
            subscriptions: std::sync::Mutex::new(Some(Vec::new())),
            authority,
        }))
    }

    /// Serves the connection until the peer closes it or breaks the protocol.
    /// The greeting must arrive within the transport timeout; after it the
    /// connection may be quiet for as long as the peer likes.
    pub fn serve(self: &Arc<Self>, dispatcher: &Arc<Dispatcher>, configuration: &ReadyCoreConfiguration) {
        if let Continuation::Going = self.greeted(configuration) {
            self.listen(dispatcher);
        }
        self.end();
    }

    fn greeted(&self, configuration: &ReadyCoreConfiguration) -> Continuation {
        use capsule_nexus_wire::exchange::{read_frame_or_end, restored};
        use signal::{Contracted, Delivery, Dispatch, Ending, ExchangeFault, Greeted, HandshakeReceipt};
        let capacity = signal::FrameCapacity::from(configuration.frame_capacity_bytes);
        let first = DeadlineStream::new(
            match self.stream.try_clone() {
                Ok(stream) => stream,
                Err(_) => return Continuation::Ended,
            },
            configuration.transport_timeout,
        )
        .ok()
        .and_then(|mut input| read_frame_or_end(&mut input, capacity).ok().flatten());
        let Some(body) = first else {
            return Continuation::Ended;
        };
        match restored::<Dispatch<capsule_nexus_wire::public_call::Input>>(body) {
            Ok(Dispatch::Greet(greeting)) => {
                let receipt = capsule_nexus_wire::public_call::Input::receipt(&greeting);
                let settled = matches!(receipt, HandshakeReceipt::Greeted(_));
                if self.deliver(Delivery::Greeted(receipt), configuration).is_err() || !settled {
                    return Continuation::Ended;
                }
                let _ = self.ledger.lock().unwrap_or_else(|p| p.into_inner()).greet();
                Continuation::Going
            }
            Ok(_) => self.faulted(Ending::connection_faulted(ExchangeFault::GreetingExpected), configuration),
            Err(_) => self.faulted(Ending::connection_faulted(ExchangeFault::UnreadableQuery), configuration),
        }
    }

    /// Reads the greeted connection's frames until it ends.
    fn listen(self: &Arc<Self>, dispatcher: &Arc<Dispatcher>) {
        use capsule_nexus_wire::exchange::{read_frame_after_idle, restored};
        use signal::{Dispatch, Ending, ExchangeFault, FrameError};
        loop {
            let Some(configuration) = dispatcher.configuration.snapshot() else {
                return;
            };
            let capacity = signal::FrameCapacity::from(configuration.frame_capacity_bytes);
            let body = match read_frame_after_idle(&self.stream, configuration.transport_timeout, capacity) {
                Ok(Some(body)) => body,
                Ok(None) | Err(FrameError::Io(_)) => return,
                Err(FrameError::BodyTooLarge { .. }) => {
                    self.faulted(Ending::connection_faulted(ExchangeFault::UnreadableQuery), &configuration);
                    return;
                }
            };
            let continuation = match restored::<Dispatch<capsule_nexus_wire::public_call::Input>>(body) {
                Err(_) => self.faulted(Ending::connection_faulted(ExchangeFault::UnreadableQuery), &configuration),
                Ok(Dispatch::Greet(_)) => {
                    self.faulted(Ending::connection_faulted(ExchangeFault::GreetingRepeated), &configuration)
                }
                Ok(Dispatch::Open(opening)) => self.opened(dispatcher, opening, configuration),
                Ok(Dispatch::Abandon(exchange)) => self.abandoned(exchange, &configuration),
            };
            if let Continuation::Ended = continuation {
                return;
            }
        }
    }

    fn opened(
        self: &Arc<Self>,
        dispatcher: &Arc<Dispatcher>,
        opening: signal::Opening<capsule_nexus_wire::public_call::Input>,
        configuration: Arc<ReadyCoreConfiguration>,
    ) -> Continuation {
        use signal::{Ending, ExchangeTracking, Exchanged};
        let exchange = opening.exchange;
        let admitted = self.ledger.lock().unwrap_or_else(|p| p.into_inner()).admit(exchange);
        if let Err(fault) = admitted {
            let ending = Ending::faulted(exchange, fault);
            if ending.is_connection_wide() {
                return self.faulted(ending, &configuration);
            }
            return match self.deliver(signal::Delivery::End(ending), &configuration) {
                Ok(()) => Continuation::Going,
                Err(_) => Continuation::Ended,
            };
        }
        let capsule_nexus_wire::public_call::Input::Call(call) = opening.query;
        // The authority is the connection's: a configured peer's binding is
        // checked again for every exchange, and a Caller that is not the
        // connection's own peer ends the connection unanswered.
        let Ok(actual) = capsule_nexus_wire::frame::peer_caller(&self.stream) else {
            return Continuation::Ended;
        };
        let peer_grant = self.authority.peer_grant();
        if let Some((peer_id, generation)) = &peer_grant {
            if !dispatcher.peer_grant_is_current(&actual, peer_id, *generation) {
                eprintln!("Capsule Nexus public request failed: configured peer binding changed before request dispatch");
                return Continuation::Ended;
            }
        }
        let principal = match principal_for(&self.authority, &actual, &call.caller) {
            Ok(principal) => principal,
            Err(error) => {
                eprintln!("Capsule Nexus public request failed: {error}");
                return Continuation::Ended;
            }
        };
        let connection = Arc::clone(self);
        let dispatcher = Arc::clone(dispatcher);
        std::thread::spawn(move || {
            match dispatcher.dispatch_as(principal.clone(), call.public_input.0, Arc::clone(&configuration)) {
                PublicDispatch::Answered(response) => connection.answer(exchange, response, &configuration),
                PublicDispatch::Subscribe(session) => {
                    if let Err(error) = dispatcher.subscribe_bound(
                        &connection,
                        exchange,
                        principal,
                        peer_grant,
                        session,
                        configuration,
                    ) {
                        eprintln!("Capsule Nexus subscription failed: {error}");
                        connection.end();
                    }
                }
            }
        });
        Continuation::Going
    }

    fn abandoned(&self, exchange: signal::ExchangeId, configuration: &ReadyCoreConfiguration) -> Continuation {
        use signal::{Ending, ExchangeTracking, Exchanged};
        let released = self.ledger.lock().unwrap_or_else(|p| p.into_inner()).release(exchange);
        match released {
            Ok(_) => {
                if let Some(queue) = self.withdrawn(exchange) {
                    queue.close(Closing::Abandoned);
                }
                Continuation::Going
            }
            Err(fault) => {
                let ending = Ending::faulted(exchange, fault);
                if ending.is_connection_wide() {
                    return self.faulted(ending, configuration);
                }
                match self.deliver(signal::Delivery::End(ending), configuration) {
                    Ok(()) => Continuation::Going,
                    Err(_) => Continuation::Ended,
                }
            }
        }
    }

    /// Answers a one-answer exchange, which ends with it. An exchange the peer
    /// abandoned meanwhile is not answered.
    fn answer(
        &self,
        exchange: signal::ExchangeId,
        response: signal_capsule_nexus::Output,
        configuration: &ReadyCoreConfiguration,
    ) {
        use signal::ExchangeTracking;
        let released = self.ledger.lock().unwrap_or_else(|p| p.into_inner()).release(exchange);
        if released.is_ok() && self.deliver(Self::answered(exchange, response), configuration).is_err() {
            self.end();
        }
    }

    /// Sends one more answer on a subscription's exchange.
    fn stream(
        &self,
        exchange: signal::ExchangeId,
        response: signal_capsule_nexus::Output,
        configuration: &ReadyCoreConfiguration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use signal::ExchangeTracking;
        if !self.ledger.lock().unwrap_or_else(|p| p.into_inner()).bears(exchange) {
            return Ok(());
        }
        self.deliver(Self::answered(exchange, response), configuration)
    }

    /// Records a subscription's queue on its exchange. A connection that has
    /// already ended abandons it at once.
    fn carry(&self, exchange: signal::ExchangeId, queue: &Arc<SubscriberQueue>) {
        match self.subscriptions.lock().unwrap_or_else(|p| p.into_inner()).as_mut() {
            Some(subscriptions) => subscriptions.push((exchange, Arc::clone(queue))),
            None => queue.close(Closing::Abandoned),
        }
    }

    fn withdrawn(&self, exchange: signal::ExchangeId) -> Option<Arc<SubscriberQueue>> {
        let mut subscriptions = self.subscriptions.lock().unwrap_or_else(|p| p.into_inner());
        let subscriptions = subscriptions.as_mut()?;
        let at = subscriptions.iter().position(|(carried, _)| *carried == exchange)?;
        Some(subscriptions.remove(at).1)
    }

    /// Ends a subscription's exchange as its queue's closing says.
    fn conclude(
        &self,
        exchange: signal::ExchangeId,
        closing: Closing,
        configuration: Option<Arc<ReadyCoreConfiguration>>,
    ) {
        use signal::{Delivery, Ending, ExchangeFault, ExchangeTracking};
        self.withdrawn(exchange);
        let released = self.ledger.lock().unwrap_or_else(|p| p.into_inner()).release(exchange);
        match (closing, configuration) {
            (Closing::Abandoned, _) => {}
            (Closing::Lagged, Some(configuration)) if released.is_ok() => {
                let ending = Ending::faulted(exchange, ExchangeFault::Lagged);
                if self.deliver(Delivery::End(ending), &configuration).is_err() {
                    self.end();
                }
            }
            (Closing::Lagged, _) => {}
            (Closing::Withdrawn, _) => self.end(),
        }
    }

    fn answered(
        exchange: signal::ExchangeId,
        response: signal_capsule_nexus::Output,
    ) -> signal::Delivery<capsule_nexus_wire::public_call::Output> {
        signal::Delivery::Answer(signal::Answer {
            exchange,
            response: capsule_nexus_wire::public_call::Output::Answered(
                capsule_nexus_wire::public_payload::Output(response),
            ),
        })
    }

    /// Ends the connection with a fault it names.
    fn faulted(&self, ending: signal::Ending, configuration: &ReadyCoreConfiguration) -> Continuation {
        let _ = self.deliver(signal::Delivery::End(ending), configuration);
        Continuation::Ended
    }

    fn deliver(
        &self,
        delivery: signal::Delivery<capsule_nexus_wire::public_call::Output>,
        configuration: &ReadyCoreConfiguration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let writing = self.writing.lock().unwrap_or_else(|p| p.into_inner());
        capsule_nexus_wire::exchange::write_value(
            &writing,
            &delivery,
            configuration.transport_timeout,
            signal::FrameCapacity::from(configuration.frame_capacity_bytes),
        )?;
        Ok(())
    }

    /// Ends the connection: every subscription on it is abandoned and the peer
    /// reads the end of the stream.
    fn end(&self) {
        let carried = self.subscriptions.lock().unwrap_or_else(|p| p.into_inner()).take();
        for (_, queue) in carried.into_iter().flatten() {
            queue.close(Closing::Abandoned);
        }
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }
}

pub fn same_login(caller: &shared_translator::Caller) -> bool {
    caller.user_id.0 == i64::from(nix::unistd::getuid().as_raw())
}

pub struct Dispatcher {
    state: std::sync::Mutex<DispatcherState>,
    emitter: Arc<dyn ExternalEmitter>,
    subscriptions: std::sync::Mutex<SubscriptionHub>,
    next_subscription_generation: std::sync::atomic::AtomicU64,
    configuration: Arc<ConfigurationStore>,
}

/// The complete public operation decision: answer once or establish the
/// dedicated subscription stream. Subscribe has no parallel one-shot path.
pub enum PublicDispatch {
    Answered(signal_capsule_nexus::Output),
    Subscribe(signal_capsule_nexus::Session),
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

struct SocketEmitter {
    translator_socket: std::path::PathBuf,
}

struct DispatcherState {
    core: capsule_nexus_core::Core<capsule_nexus_core::InMemory>,
    principals: std::collections::HashMap<signal_capsule_nexus::Pane, shared_translator::Principal>,
    reserved: std::collections::HashMap<signal_capsule_nexus::Pane, u64>,
    revisions: std::collections::HashMap<signal_capsule_nexus::Pane, u64>,
    next_reservation: u64,
    instance_id: signal_capsule_nexus::InstanceId,
    next_creation_sequence: i64,
}

#[derive(Default)]
struct SubscriptionHub {
    sessions: std::collections::HashMap<(shared_translator::Principal, signal_capsule_nexus::Session), SessionSubscription>,
}

struct SessionSubscription {
    generation: u64,
    subscribers: Vec<Subscriber>,
}

struct Subscriber {
    queue: Arc<SubscriberQueue>,
    delivered: std::collections::HashMap<signal_capsule_nexus::Pane, signal_capsule_nexus::State>,
}

struct SubscriberQueue {
    state: std::sync::Mutex<SubscriberQueueState>,
    ready: std::sync::Condvar,
}

struct SubscriberQueueState {
    pending: std::collections::VecDeque<signal_capsule_nexus::Output>,
    capacity: usize,
    closing: Option<Closing>,
}

/// Why a subscription stops being fed, which decides how its exchange ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Closing {
    /// The subscriber fell further behind than its queue holds: the exchange
    /// ends `Faulted.Lagged`, and the subscriber opens Subscribe again.
    Lagged,
    /// The authority the subscription was opened under is gone: its peer was
    /// rebound or removed, or its connection's writing failed. The
    /// connection closes.
    Withdrawn,
    /// The querying side abandoned the exchange or closed the connection.
    Abandoned,
}

enum QueueNext {
    Response(signal_capsule_nexus::Output),
    Closed(Closing),
}

impl SubscriberQueue {
    fn new(capacity: usize) -> Self {
        Self {
            state: std::sync::Mutex::new(SubscriberQueueState {
                pending: std::collections::VecDeque::new(),
                capacity,
                closing: None,
            }),
            ready: std::sync::Condvar::new(),
        }
    }

    fn enqueue(&self, response: signal_capsule_nexus::Output) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.closing.is_some() {
            return false;
        }
        if state.pending.len() >= state.capacity {
            state.closing = Some(Closing::Lagged);
            state.pending.clear();
            self.ready.notify_all();
            return false;
        }
        state.pending.push_back(response);
        self.ready.notify_one();
        true
    }

    /// Waits for the next response, or for the queue to close.
    fn next(&self) -> QueueNext {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        while state.pending.is_empty() && state.closing.is_none() {
            state = self.ready.wait(state).unwrap_or_else(|p| p.into_inner());
        }
        match state.closing {
            Some(closing) => QueueNext::Closed(closing),
            None => QueueNext::Response(state.pending.pop_front().expect("nonempty subscriber queue")),
        }
    }

    fn set_capacity(&self, capacity: usize) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.capacity = capacity;
        if state.pending.len() > capacity {
            state.closing.get_or_insert(Closing::Lagged);
            state.pending.clear();
            self.ready.notify_all();
        }
    }

    /// Closes the queue for `closing`, unless it is already closed for
    /// another reason, which stands.
    fn close(&self, closing: Closing) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.closing.get_or_insert(closing);
        state.pending.clear();
        self.ready.notify_all();
    }

    fn is_open(&self) -> bool {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).closing.is_none()
    }
}

fn operation(
    query: signal_capsule_nexus::Input,
) -> capsule_nexus_core::generated::operation::Operation {
    use capsule_nexus_core::generated::operation::{
        Operation, Pressing_Data, Prompting_Data, Starting_Data,
    };
    match query {
        signal_capsule_nexus::Input::Create(workspace) => Operation::Creating(workspace),
        signal_capsule_nexus::Input::Start(start) => Operation::Starting(Starting_Data {
            pane: start.pane,
            command: start.command,
            label: start.label,
        }),
        signal_capsule_nexus::Input::Prompt(prompt) => Operation::Prompting(Prompting_Data {
            pane: prompt.pane,
            text: prompt.text,
        }),
        signal_capsule_nexus::Input::Press(press) => Operation::Pressing(Pressing_Data {
            pane: press.pane,
            keys: press.keys,
        }),
        signal_capsule_nexus::Input::Close(pane) => Operation::Closing(pane),
        signal_capsule_nexus::Input::List(session) => Operation::Listing(session),
        signal_capsule_nexus::Input::Subscribe(session) => Operation::Subscribing(session),
    }
}

impl Dispatcher {
    pub fn new(
        paths: &SocketPaths,
        configuration: Arc<ConfigurationStore>,
        instance_id: signal_capsule_nexus::InstanceId,
    ) -> Self {
        Self {
            state: std::sync::Mutex::new(DispatcherState {
                core: capsule_nexus_core::Core::new(capsule_nexus_core::InMemory::default()),
                principals: std::collections::HashMap::new(),
                reserved: std::collections::HashMap::new(),
                revisions: std::collections::HashMap::new(),
                next_reservation: 1,
                instance_id,
                next_creation_sequence: 1,
            }),
            emitter: Arc::new(SocketEmitter {
                translator_socket: paths.translator.clone(),
            }),
            subscriptions: std::sync::Mutex::new(SubscriptionHub::default()),
            next_subscription_generation: std::sync::atomic::AtomicU64::new(1),
            configuration,
        }
    }

    pub fn dispatch_as(
        &self,
        principal: shared_translator::Principal,
        query: signal_capsule_nexus::Input,
        configuration: Arc<ReadyCoreConfiguration>,
    ) -> PublicDispatch {
        use emission_capsule_nexus::{
            AgentPrompt_Data, AgentStart_Data, Herdr, SendKeys_Data, Verb, WorkspaceCreate_Data,
        };
        use capsule_nexus_core::generated::operation::Operation;
        use signal_capsule_nexus::Output;
        let response = match operation(query) {
            Operation::Creating(workspace) => {
                let creation_id = {
                    let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                    let creation_id = signal_capsule_nexus::CreationId {
                        instance_id: state.instance_id.clone(),
                        sequence: signal_capsule_nexus::Sequence(state.next_creation_sequence),
                    };
                    let Some(next) = state.next_creation_sequence.checked_add(1) else {
                        return PublicDispatch::Answered(Output::Created(signal_capsule_nexus::Outcome::StoreRefused(
                            signal_capsule_nexus::StoreRefusal {
                                memory_refusal: signal_capsule_nexus::MemoryRefusal(
                                    signal_capsule_nexus::Message("creation sequence exhausted".into()),
                                ),
                                store_phase: signal_capsule_nexus::StorePhase::BeforeEmission,
                            },
                        )));
                    };
                    let pending = capsule_nexus_core::generated::memory::PendingCreation {
                        creation_id: creation_id.clone(),
                        principal: principal.clone(),
                        workspace: workspace.clone(),
                    };
                    if let Err(outcome) = state.core.prepare_creation(pending) {
                        return PublicDispatch::Answered(Output::Created(outcome));
                    }
                    state.next_creation_sequence = next;
                    creation_id
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
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                let outcome = state.core.create(creation_id, created_result(external));
                if let signal_capsule_nexus::Outcome::Created(pane) = &outcome {
                    state.revisions.insert(pane.clone(), 1);
                    state.principals.insert(pane.clone(), principal);
                }
                Output::Created(outcome)
            }
            Operation::Starting(start) => {
                let reservation = match self.reserve(&principal, &start.pane) {
                    Ok(token) => token,
                    Err(outcome) => return PublicDispatch::Answered(Output::Started(outcome)),
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
                let mut state = self.finish_reservation(&start.pane, reservation);
                let pane = start.pane;
                let outcome = state
                    .core
                    .start(pane.clone(), command, start.label, started_result(external));
                if matches!(
                    outcome,
                    signal_capsule_nexus::Outcome::Done
                        | signal_capsule_nexus::Outcome::Uncertain
                ) {
                    bump_revision(&mut state.revisions, pane);
                }
                Output::Started(outcome)
            }
            Operation::Prompting(prompt) => {
                let reservation = match self.reserve(&principal, &prompt.pane) {
                    Ok(token) => token,
                    Err(outcome) => return PublicDispatch::Answered(Output::Prompted(outcome)),
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
                let mut state = self.finish_reservation(&prompt.pane, reservation);
                let pane = prompt.pane;
                let outcome = state.core.prompt(pane.clone(), text, done_result(external));
                if matches!(
                    outcome,
                    signal_capsule_nexus::Outcome::Done
                        | signal_capsule_nexus::Outcome::Uncertain
                ) {
                    bump_revision(&mut state.revisions, pane);
                }
                Output::Prompted(outcome)
            }
            Operation::Pressing(press) => {
                let reservation = match self.reserve(&principal, &press.pane) {
                    Ok(token) => token,
                    Err(outcome) => return PublicDispatch::Answered(Output::Pressed(outcome)),
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
                let mut state = self.finish_reservation(&press.pane, reservation);
                let pane = press.pane;
                let outcome = state.core.press(pane.clone(), keys, done_result(external));
                if matches!(
                    outcome,
                    signal_capsule_nexus::Outcome::Done
                        | signal_capsule_nexus::Outcome::Uncertain
                ) {
                    bump_revision(&mut state.revisions, pane);
                }
                Output::Pressed(outcome)
            }
            Operation::Closing(pane) => {
                let reservation = match self.reserve(&principal, &pane) {
                    Ok(token) => token,
                    Err(outcome) => return PublicDispatch::Answered(Output::Closed(outcome)),
                };
                let external = self.execute(
                    &configuration,
                    Herdr {
                        session: pane.session.clone(),
                        verb: Verb::PaneClose(pane.pane_id.clone()),
                    },
                );
                let mut state = self.finish_reservation(&pane, reservation);
                let externally_closed = matches!(
                    &external,
                    EmissionResult::External(emission_capsule_nexus::ExternalOutcome::Done)
                ) || is_absent_close(&external);
                if externally_closed {
                    self.publish_closed(&principal, &pane);
                }
                let outcome = state.core.close(pane.clone(), closed_result(external));
                if outcome == signal_capsule_nexus::Outcome::Done {
                    state.revisions.remove(&pane);
                    state.principals.remove(&pane);
                } else if outcome == signal_capsule_nexus::Outcome::Uncertain {
                    bump_revision(&mut state.revisions, pane);
                }
                Output::Closed(outcome)
            }
            Operation::Listing(session) => {
                let (captured, pending) = {
                    let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                    let owned = match state.core.owned_in_session(&session) {
                        Ok(owned) => owned,
                        Err(outcome) => return PublicDispatch::Answered(Output::Listed(outcome)),
                    };
                    let pending = match state.core.pending_in_session(&session) {
                        Ok(pending) => pending,
                        Err(outcome) => return PublicDispatch::Answered(Output::Listed(outcome)),
                    };
                    let owns_any = owned.iter().any(|owned| {
                        state.principals.get(&owned.pane) == Some(&principal)
                    }) || pending.iter().any(|creation| creation.principal == principal);
                    if !owns_any {
                        return PublicDispatch::Answered(Output::Listed(signal_capsule_nexus::Outcome::Listed(Vec::new())));
                    }
                    (state.revisions.clone(), pending)
                };
                let panes = self.execute(
                    &configuration,
                    Herdr {
                        session: session.clone(),
                        verb: Verb::PaneList,
                    },
                );
                let (pane_listings, external) = match pane_listing_result(panes) {
                    Ok(panes) => {
                        let agents = self.execute(
                            &configuration,
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
                        &configuration,
                        Herdr {
                            session: session.clone(),
                            verb: Verb::WorkspaceList,
                        },
                    ))
                    .ok()
                };
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                if let (Some(panes), Some(workspaces)) = (&pane_listings, &workspace_listings) {
                    adopt_pending_creations(&mut state, &pending, panes, workspaces);
                }
                let external = protect_newer_state(&state, &session, &captured, external);
                let outcome = state.core.list(session.clone(), external);
                let outcome = filter_listing(outcome, &principal, &state.principals);
                if matches!(outcome, signal_capsule_nexus::Outcome::Listed(_)) {
                    if let Ok(owned) = state.core.owned_in_session(&session) {
                        for owned in owned {
                            bump_revision(&mut state.revisions, owned.pane);
                        }
                    }
                }
                Output::Listed(outcome)
            }
            Operation::Subscribing(session) => return PublicDispatch::Subscribe(session),
        };
        PublicDispatch::Answered(response)
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

    fn reserve(&self, principal: &shared_translator::Principal, pane: &signal_capsule_nexus::Pane) -> Result<u64, signal_capsule_nexus::Outcome> {
        use signal_capsule_nexus::Outcome;
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.core.preflight_owned(pane)?;
        if state.principals.get(pane) != Some(principal) {
            return Err(Outcome::Unknown(pane.clone()));
        }
        if state.reserved.contains_key(pane) {
            return Err(Outcome::AttemptInProgress(pane.clone()));
        }
        let token = state.next_reservation;
        state.next_reservation = state.next_reservation.wrapping_add(1).max(1);
        state.reserved.insert(pane.clone(), token);
        Ok(token)
    }

    fn finish_reservation(
        &self,
        pane: &signal_capsule_nexus::Pane,
        token: u64,
    ) -> std::sync::MutexGuard<'_, DispatcherState> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let owns = state.reserved.get(pane).copied() == Some(token);
        assert!(owns, "only the reservation token owner may finalize it");
        state.reserved.remove(pane);
        state
    }

    /// Opens a nondurable subscription on an exchange of `connection` and
    /// registers it in the session's shared poll source. Its acknowledgement
    /// and the current state are the exchange's first answers, and each change
    /// after them is one more, until its queue closes and the exchange ends as
    /// the `Closing` says. Each subscriber has one bounded FIFO and one writer;
    /// publishing never waits for socket I/O.
    pub fn subscribe_bound(
        self: &Arc<Self>,
        connection: &Arc<PublicConnection>,
        exchange: signal::ExchangeId,
        principal: shared_translator::Principal,
        peer_grant: Option<(shared_translator::PeerId, u64)>,
        session: signal_capsule_nexus::Session,
        configuration: Arc<ReadyCoreConfiguration>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use signal_capsule_nexus::{Changed_Data, Output};
        let (acknowledgement, current) = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let acknowledgement = Output::Subscribed(state.core.subscribe(
                session.clone(),
                capsule_nexus_core::ExternalResult::Succeeded(()),
            ));
            let current = state
                .core
                .owned_in_session(&session)
                .map_err(|_| std::io::Error::other("Memory refused subscription snapshot"))?
                .into_iter()
                .filter(|known| state.principals.get(&known.pane) == Some(&principal))
                .map(|known| (known.pane, known.state))
                .collect::<std::collections::HashMap<_, _>>();
            (acknowledgement, current)
        };
        let queue = Arc::new(SubscriberQueue::new(configuration.subscriber_capacity));
        if !queue.enqueue(acknowledgement) {
            return Err("subscriber queue refused its acknowledgement".into());
        }
        for (pane, state) in &current {
            if !queue.enqueue(Output::Changed(Changed_Data {
                    pane: pane.clone(),
                    state: state.clone(),
                })) {
                return Err("subscriber queue cannot hold its initial snapshot".into());
            }
        }
        self.configuration.register_subscriber(&queue);
        if let shared_translator::Principal::Peer(peer_id) = &principal {
            let Some((granted_id, generation)) = peer_grant else {
                queue.close(Closing::Withdrawn);
                return Err("configured peer subscription lacks its binding generation".into());
            };
            if granted_id != *peer_id
                || !self.configuration.register_peer_subscriber(peer_id, generation, &queue)
            {
                queue.close(Closing::Withdrawn);
                return Err("configured peer binding changed before subscription registration".into());
            }
        }
        connection.carry(exchange, &queue);
        let writer_queue = Arc::clone(&queue);
        let writer_configuration = Arc::clone(&self.configuration);
        let writer_connection = Arc::clone(connection);
        std::thread::spawn(move || loop {
            match writer_queue.next() {
                QueueNext::Closed(closing) => {
                    writer_connection.conclude(exchange, closing, writer_configuration.snapshot());
                    return;
                }
                QueueNext::Response(response) => {
                    let Some(configuration) = writer_configuration.snapshot() else {
                        writer_queue.close(Closing::Withdrawn);
                        continue;
                    };
                    if writer_connection.stream(exchange, response, &configuration).is_err() {
                        writer_queue.close(Closing::Withdrawn);
                    }
                }
            }
        });
        let mut hub = self
            .subscriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = (principal.clone(), session.clone());
        if let Some(existing) = hub.sessions.get_mut(&key) {
            existing.subscribers.push(Subscriber {
                queue,
                delivered: current,
            });
            return Ok(());
        }
        let generation = self
            .next_subscription_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        hub.sessions.insert(
            key,
            SessionSubscription {
                generation,
                subscribers: vec![Subscriber {
                    queue,
                    delivered: current,
                }],
            },
        );
        drop(hub);
        let dispatcher = Arc::clone(self);
        std::thread::spawn(move || dispatcher.poll_subscription(principal, session, generation));
        Ok(())
    }

    fn poll_subscription(self: Arc<Self>, principal: shared_translator::Principal, session: signal_capsule_nexus::Session, generation: u64) {
        use signal_capsule_nexus::{Changed_Data, Outcome, Input, Output};
        loop {
            let Some(configuration) = self.configuration.snapshot() else {
                return;
            };
            {
                let hub = self
                    .subscriptions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let Some(subscription) = hub.sessions.get(&(principal.clone(), session.clone())) else {
                    return;
                };
                if subscription.generation != generation {
                    return;
                }
            }
            std::thread::sleep(configuration.polling);
            {
                let mut hub = self
                    .subscriptions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let Some(subscription) = hub.sessions.get_mut(&(principal.clone(), session.clone())) else {
                    return;
                };
                if subscription.generation != generation {
                    return;
                }
                subscription.subscribers.retain(|subscriber| subscriber.queue.is_open());
                if subscription.subscribers.is_empty() {
                    hub.sessions.remove(&(principal.clone(), session.clone()));
                    return;
                }
            }
            let response = match self.dispatch_as(
                principal.clone(),
                Input::List(session.clone()),
                Arc::clone(&configuration),
            ) {
                PublicDispatch::Answered(response) => response,
                PublicDispatch::Subscribe(_) => return,
            };
            let complete = matches!(response, Output::Listed(Outcome::Listed(_)));
            // Hold Memory stable through publication. Close uses the same
            // state-before-hub order, so an older poll can never reopen a
            // pane after its terminal Dead event.
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let current = if complete {
                state.core.owned_in_session(&session).ok().map(|known| {
                    known
                        .into_iter()
                        .filter(|known| state.principals.get(&known.pane) == Some(&principal))
                        .map(|known| (known.pane, known.state))
                        .collect::<std::collections::HashMap<_, _>>()
                })
            } else {
                None
            };
            let mut hub = self
                .subscriptions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(subscription) = hub.sessions.get_mut(&(principal.clone(), session.clone())) else {
                return;
            };
            if subscription.generation != generation {
                return;
            }
            if let Some(current) = current {
                subscription.subscribers.retain_mut(|subscriber| {
                    let changes = current
                        .iter()
                        .filter(|(pane, state)| subscriber.delivered.get(*pane) != Some(*state))
                        .map(|(pane, state)| {
                            Output::Changed(Changed_Data {
                                pane: pane.clone(),
                                state: state.clone(),
                            })
                        })
                        .collect::<Vec<_>>();
                    let delivered = changes
                        .into_iter()
                        .all(|change| subscriber.queue.enqueue(change));
                    if delivered {
                        subscriber.delivered = current.clone();
                    }
                    delivered
                });
            }
            if subscription.subscribers.is_empty() {
                hub.sessions.remove(&(principal.clone(), session.clone()));
                return;
            }
            drop(hub);
            drop(state);
        }
    }

    fn execute(
        &self,
        configuration: &ReadyCoreConfiguration,
        herdr: emission_capsule_nexus::Herdr,
    ) -> EmissionResult {
        self.emitter.emit(configuration, herdr)
    }

    fn publish_closed(&self, principal: &shared_translator::Principal, pane: &signal_capsule_nexus::Pane) {
        use signal_capsule_nexus::{Changed_Data, Output, State};
        let mut hub = self
            .subscriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(subscription) = hub.sessions.get_mut(&(principal.clone(), pane.session.clone())) else {
            return;
        };
        subscription.subscribers.retain_mut(|subscriber| {
            if subscriber.delivered.get(pane) == Some(&State::Dead) {
                return subscriber.queue.is_open();
            }
            let delivered = subscriber.queue.enqueue(Output::Changed(Changed_Data {
                pane: pane.clone(),
                state: State::Dead,
            }));
            if delivered {
                subscriber.delivered.insert(pane.clone(), State::Dead);
            }
            delivered
        });
    }
}

impl ExternalEmitter for SocketEmitter {
    fn emit(
        &self,
        configuration: &ReadyCoreConfiguration,
        herdr: emission_capsule_nexus::Herdr,
    ) -> EmissionResult {
        use capsule_nexus_wire::transport_types::{Kind, Lane};
        use signal::{ByteViewable, Restorable, Signal, Signalizable};
        let stream = match std::os::unix::net::UnixStream::connect(&self.translator_socket) {
            Ok(stream) => stream,
            Err(error) => return unavailable_transport(&self.translator_socket, error.to_string()),
        };
        let response_stream = match stream.try_clone() {
            Ok(stream) => stream,
            Err(error) => return unavailable_transport(&self.translator_socket, error.to_string()),
        };
        let mut output = match DeadlineStream::new(stream, configuration.transport_timeout) {
            Ok(stream) => stream,
            Err(error) => return unavailable_transport(&self.translator_socket, error.to_string()),
        };
        let request = emission_capsule_nexus::Input::Emit(herdr);
        let request = match request.signalize() {
            Ok(request) => request,
            Err(error) => {
                return unavailable_transport(&self.translator_socket, format!("{error:?}"))
            }
        };
        if let Err(failure) = capsule_nexus_wire::frame::write_frame_witness(
            &mut output,
            &capsule_nexus_wire::frame::Frame {
                lane: Lane::EmissionSignal,
                kind: Kind::Request,
                payload: request.bytes().to_vec(),
            },
            configuration.frame_capacity_bytes,
        ) {
            return unavailable_transport(
                &self.translator_socket,
                format!(
                    "incomplete request submission after {} bytes: {:?}",
                    failure.written, failure.error
                ),
            );
        }
        drop(output);
        let result = (|| -> Result<_, Box<dyn std::error::Error>> {
            let body = capsule_nexus_wire::frame::read_frame_after_idle(
                response_stream,
                configuration.transport_timeout,
                configuration.frame_capacity_bytes,
            )?
            .ok_or("translator closed without an emission answer")?;
            if body.lane != Lane::EmissionSignal || body.kind != Kind::Answered {
                return Err("translator returned a non-answer emission frame".into());
            }
            let response =
                Signal::<emission_capsule_nexus::Output>::from(body.payload).restore()?;
            match response {
                emission_capsule_nexus::Output::Emitted(outcome) => Ok(outcome),
            }
        })();
        EmissionResult::External(result.unwrap_or(emission_capsule_nexus::ExternalOutcome::Uncertain))
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
    state: &DispatcherState,
    session: &signal_capsule_nexus::Session,
    captured: &std::collections::HashMap<signal_capsule_nexus::Pane, u64>,
    external: capsule_nexus_core::ExternalResult<Vec<emission_capsule_nexus::PaneObservation>>,
) -> capsule_nexus_core::ExternalResult<Vec<emission_capsule_nexus::PaneObservation>> {
    let capsule_nexus_core::ExternalResult::Succeeded(mut observed) = external else {
        return external;
    };
    let Ok(owned) = state.core.owned_in_session(session) else {
        return capsule_nexus_core::ExternalResult::Succeeded(observed);
    };
    for current in owned {
        let changed = captured.get(&current.pane) != state.revisions.get(&current.pane)
            || state.reserved.contains_key(&current.pane);
        if changed {
            observed.retain(|item| item.pane != current.pane);
            observed.push(emission_capsule_nexus::PaneObservation {
                pane: current.pane,
                state: current.state,
            });
        }
    }
    capsule_nexus_core::ExternalResult::Succeeded(observed)
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
    state: &mut DispatcherState,
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
    pub subscriber_capacity: usize,
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
    subscriber_queues: std::sync::Mutex<Vec<std::sync::Weak<SubscriberQueue>>>,
    peer_subscriber_queues: std::sync::Mutex<std::collections::HashMap<shared_translator::PeerId, Vec<std::sync::Weak<SubscriberQueue>>>>,
    peers: std::sync::Mutex<std::collections::HashMap<shared_translator::PeerId, BoundPeer>>,
    next_peer_generation: std::sync::atomic::AtomicU64,
}

struct BoundPeer {
    live: shared_translator::unix::LivePeer,
    generation: u64,
}

impl BoundPeer {
    fn capture(identity: shared_translator::PeerIdentity) -> std::io::Result<Self> {
        shared_translator::unix::LivePeer::capture(identity)
            .map(|live| Self { live, generation: 0 })
    }

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
                Some(subscriber_capacity),
            ) => Some(ReadyCoreConfiguration {
                polling: *polling,
                start_timeout: *start_timeout,
                frame_capacity_bytes: *frame_capacity_bytes,
                transport_timeout: *transport_timeout,
                subscriber_capacity: *subscriber_capacity,
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
            self.apply_subscriber_capacity(validated.subscriber_capacity);
            *self.current.write().unwrap_or_else(|p| p.into_inner()) = Some(Arc::new(validated));
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
        if let Some(queues) = self.peer_subscriber_queues
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(peer_id)
        {
            for queue in queues.into_iter().filter_map(|queue| queue.upgrade()) {
                queue.close(Closing::Withdrawn);
            }
        }
    }

    fn register_peer_subscriber(&self, peer_id: &shared_translator::PeerId, generation: u64, queue: &Arc<SubscriberQueue>) -> bool {
        self.register_peer_subscriber_after_validation(peer_id, generation, queue, || {})
    }

    fn register_peer_subscriber_after_validation(
        &self,
        peer_id: &shared_translator::PeerId,
        generation: u64,
        queue: &Arc<SubscriberQueue>,
        after_validation: impl FnOnce(),
    ) -> bool {
        let peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
        if peers.get(peer_id).map(|peer| peer.generation) != Some(generation) {
            queue.close(Closing::Withdrawn);
            return false;
        }
        // Rebinding takes the same peers lock before it removes and closes the
        // old generation's queues.  Retaining this guard through insertion
        // makes validation and registration one transaction with invalidation.
        after_validation();
        self.peer_subscriber_queues
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(peer_id.clone())
            .or_default()
            .push(Arc::downgrade(queue));
        true
    }

    pub fn snapshot(&self) -> Option<Arc<ReadyCoreConfiguration>> {
        self.current
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    fn register_subscriber(&self, queue: &Arc<SubscriberQueue>) {
        self.subscriber_queues
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(Arc::downgrade(queue));
    }

    fn apply_subscriber_capacity(&self, capacity: usize) {
        let mut queues = self
            .subscriber_queues
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        queues.retain(|queue| {
            if let Some(queue) = queue.upgrade() {
                queue.set_capacity(capacity);
                true
            } else {
                false
            }
        });
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

    impl AnsweredForTest for PublicDispatch {
        fn answered(self) -> Option<signal_capsule_nexus::Output> {
            match self {
                Self::Answered(response) => Some(response),
                Self::Subscribe(_) => None,
            }
        }
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
            self: &Arc<Self>,
            stream: std::os::unix::net::UnixStream,
            session: signal_capsule_nexus::Session,
            configuration: Arc<ReadyCoreConfiguration>,
        ) -> Result<(), Box<dyn std::error::Error>>;
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
            );
            dispatcher.emitter = emitter;
            dispatcher
        }

        fn dispatch(
            &self,
            query: signal_capsule_nexus::Input,
            configuration: Arc<ReadyCoreConfiguration>,
        ) -> signal_capsule_nexus::Output {
            self.dispatch_as(
                shared_translator::Principal::Login(shared_translator::UserId(i64::from(
                    nix::unistd::getuid().as_raw(),
                ))),
                query,
                configuration,
            )
            .answered()
            .expect("test helper received a streaming subscription")
        }

        fn subscription_source_count(&self) -> usize {
            self.subscriptions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .sessions
                .len()
        }

        /// A greeted connection on `stream` whose first exchange is a
        /// Subscribe from the login, served as the public socket serves it.
        fn subscribe(
            self: &Arc<Self>,
            stream: std::os::unix::net::UnixStream,
            session: signal_capsule_nexus::Session,
            configuration: Arc<ReadyCoreConfiguration>,
        ) -> Result<(), Box<dyn std::error::Error>> {
            use signal::{ExchangeTracking, Greeted};
            let connection = PublicConnection::new(stream, PublicAuthority::Translator)?;
            {
                let mut ledger = connection.ledger.lock().expect("ledger");
                ledger.greet().expect("a fresh connection is greeted once");
                ledger.admit(signal::FIRST_EXCHANGE).expect("the first exchange is admitted");
            }
            self.subscribe_bound(
                &connection,
                signal::FIRST_EXCHANGE,
                shared_translator::Principal::Login(shared_translator::UserId(i64::from(
                    nix::unistd::getuid().as_raw(),
                ))),
                None,
                session,
                configuration,
            )?;
            let dispatcher = Arc::clone(self);
            std::thread::spawn(move || {
                connection.listen(&dispatcher);
                connection.end();
            });
            Ok(())
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
        SocketPaths {
            public: "/tmp/herdr-test-public".into(),
            meta: "/tmp/herdr-test-meta".into(),
            translator: "/tmp/herdr-test-translator".into(),
            meta_translator: "/tmp/herdr-test-meta-translator".into(),
        }
    }

    fn test_configuration() -> Arc<ReadyCoreConfiguration> {
        Arc::new(ReadyCoreConfiguration {
            polling: Duration::from_millis(10),
            start_timeout: Duration::from_millis(20),
            frame_capacity_bytes: 4096,
            transport_timeout: Duration::from_millis(30),
            subscriber_capacity: 8,
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
        let queue = Arc::new(SubscriberQueue::new(1));
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
        let stale = Arc::new(SubscriberQueue::new(1));
        assert!(!store.register_peer_subscriber(&peer_id, old_generation, &stale));
        assert!(!stale.is_open());
        let (_, new_generation) = store.peer_for(&caller).expect("replacement binding admitted");
        let current = Arc::new(SubscriberQueue::new(1));
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
        let stale = Arc::new(SubscriberQueue::new(1));
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
        let current = Arc::new(SubscriberQueue::new(1));
        assert!(store.register_peer_subscriber(&peer_id, new_generation, &current));
        assert!(current.is_open(), "the current generation remains subscribed");
    }

    #[test]
    fn socket_emitter_waits_idle_then_bounds_progress_and_capacity() {
        use emission_capsule_nexus::{ExternalOutcome, Herdr, Verb};
        use capsule_nexus_wire::transport_types::{Kind, Lane};
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
            let request = capsule_nexus_wire::frame::read_frame(
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
            capsule_nexus_wire::frame::write_frame(
                &mut stream,
                &capsule_nexus_wire::frame::Frame {
                    lane: Lane::EmissionSignal,
                    kind: Kind::Answered,
                    payload: answer.bytes().to_vec(),
                },
                signal::MAXIMUM_SIGNAL_BYTES,
            )
            .expect("emission answer");
        });
        let emitter = SocketEmitter {
            translator_socket: timeout_socket,
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
            translator_socket: capacity_socket,
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
        Arc<Dispatcher>,
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
        let dispatcher = Arc::new(Dispatcher::with_emitter(&test_paths(), emitter.clone()));
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
    fn subscription_acks_then_reports_current_and_one_harness_loss() {
        use emission_capsule_nexus::ExternalOutcome;
        let (dispatcher, _emitter, pane) = started_dispatcher(vec![
            ExternalOutcome::Uncertain,
            ExternalOutcome::Agents(vec![agent_listing(
                &pane_for("subscription", "pane"),
                signal_capsule_nexus::State::Open,
            )]),
        ]);
        let (server, mut client) = std::os::unix::net::UnixStream::pair().expect("pair");
        client
            .set_read_timeout(Some(Duration::from_millis(250)))
            .expect("timeout");
        dispatcher
            .subscribe(server, pane.session.clone(), test_configuration())
            .expect("subscribe");
        assert!(
            matches!(read_subscription(&mut client), capsule_nexus_wire::public_call::Output::Answered(ref output) if matches!(output.0, signal_capsule_nexus::Output::Subscribed(signal_capsule_nexus::Outcome::Done)))
        );
        assert!(
            matches!(read_subscription(&mut client), capsule_nexus_wire::public_call::Output::Answered(ref output) if matches!(&output.0, signal_capsule_nexus::Output::Changed(change) if change.pane == pane && change.state == signal_capsule_nexus::State::Busy))
        );
        assert!(
            matches!(read_subscription(&mut client), capsule_nexus_wire::public_call::Output::Answered(ref output) if matches!(&output.0, signal_capsule_nexus::Output::Changed(change) if change.pane == pane && change.state == signal_capsule_nexus::State::Dead))
        );
        client
            .set_read_timeout(Some(Duration::from_millis(35)))
            .expect("short timeout");
        use signal::FrameReading;
        assert!(
            client
                .read_frame(signal::FrameCapacity::from(4096))
                .is_err(),
            "repeated loss must not duplicate Dead"
        );
    }

    fn pane_for(session: &str, pane: &str) -> signal_capsule_nexus::Pane {
        signal_capsule_nexus::Pane {
            session: signal_capsule_nexus::Session(session.into()),
            pane_id: signal_capsule_nexus::PaneId(pane.into()),
        }
    }

    #[test]
    fn two_subscribers_share_one_session_poll_source() {
        let (dispatcher, emitter, pane) = started_dispatcher(Vec::new());
        let (server_one, _client_one) = std::os::unix::net::UnixStream::pair().expect("pair one");
        let (server_two, _client_two) = std::os::unix::net::UnixStream::pair().expect("pair two");
        let baseline = emitter.list_count.load(std::sync::atomic::Ordering::SeqCst);
        let started = std::time::Instant::now();
        dispatcher
            .subscribe(server_one, pane.session.clone(), test_configuration())
            .expect("first");
        dispatcher
            .subscribe(server_two, pane.session.clone(), test_configuration())
            .expect("second");
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
        // One source polls once per interval, plus its first immediate poll;
        // two independent sources would poll about twice as often. The bound
        // scales with the time actually elapsed, so load only loosens it.
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
        let (dispatcher, _emitter, pane) = started_dispatcher(Vec::new());
        let (server, client) = std::os::unix::net::UnixStream::pair().expect("pair");
        dispatcher
            .subscribe(server, pane.session.clone(), test_configuration())
            .expect("subscribe");
        drop(client);
        let deadline = std::time::Instant::now() + Duration::from_millis(250);
        while dispatcher.subscription_source_count() != 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(dispatcher.subscription_source_count(), 0);
    }

    #[test]
    fn late_subscriber_cursor_does_not_repeat_its_current_state() {
        use emission_capsule_nexus::ExternalOutcome;
        use signal_capsule_nexus::{AgentKind, AgentName, Command, Executable, Label, Program, Input, Start_Data};
        let pane = pane_for("subscription", "pane");
        let (dispatcher, _emitter, _) = started_dispatcher(vec![
            ExternalOutcome::Agents(vec![agent_listing(&pane, signal_capsule_nexus::State::Idle)]),
            ExternalOutcome::Agents(vec![agent_listing(&pane, signal_capsule_nexus::State::Busy)]),
        ]);
        let slow = Arc::new(ReadyCoreConfiguration {
            polling: Duration::from_millis(100),
            start_timeout: Duration::from_millis(20),
            frame_capacity_bytes: 4096,
            transport_timeout: Duration::from_millis(30),
            subscriber_capacity: 8,
        });
        *dispatcher.configuration.current.write().expect("configuration") = Some(Arc::clone(&slow));
        let (server_a, mut client_a) = std::os::unix::net::UnixStream::pair().expect("pair a");
        client_a.set_read_timeout(Some(Duration::from_millis(300))).expect("timeout a");
        dispatcher.subscribe(server_a, pane.session.clone(), Arc::clone(&slow)).expect("subscribe a");
        let _ = read_subscription(&mut client_a); // acknowledgement
        let _ = read_subscription(&mut client_a); // current Busy
        assert!(matches!(read_subscription(&mut client_a), capsule_nexus_wire::public_call::Output::Answered(ref output) if matches!(&output.0, signal_capsule_nexus::Output::Changed(change) if change.state == signal_capsule_nexus::State::Idle)));

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
        let (server_b, mut client_b) = std::os::unix::net::UnixStream::pair().expect("pair b");
        client_b.set_read_timeout(Some(Duration::from_millis(300))).expect("timeout b");
        dispatcher.subscribe(server_b, pane.session.clone(), Arc::clone(&slow)).expect("subscribe b");
        let _ = read_subscription(&mut client_b); // acknowledgement
        assert!(matches!(read_subscription(&mut client_b), capsule_nexus_wire::public_call::Output::Answered(ref output) if matches!(&output.0, signal_capsule_nexus::Output::Changed(change) if change.state == signal_capsule_nexus::State::Busy)));
        assert!(matches!(read_subscription(&mut client_a), capsule_nexus_wire::public_call::Output::Answered(ref output) if matches!(&output.0, signal_capsule_nexus::Output::Changed(change) if change.state == signal_capsule_nexus::State::Busy)));
        client_b.set_read_timeout(Some(Duration::from_millis(35))).expect("short timeout b");
        use signal::FrameReading;
        assert!(client_b.read_frame(signal::FrameCapacity::from(4096)).is_err(), "late subscriber must not receive its current Busy twice");
    }

    #[test]
    fn active_subscription_reads_replaced_configuration() {
        let (dispatcher, emitter, pane) = started_dispatcher(Vec::new());
        let (server, _client) = std::os::unix::net::UnixStream::pair().expect("pair");
        dispatcher.subscribe(server, pane.session, test_configuration()).expect("subscribe");
        while emitter.list_count.load(std::sync::atomic::Ordering::SeqCst) < 1 {
            std::thread::sleep(Duration::from_millis(2));
        }
        let replacement = Arc::new(ReadyCoreConfiguration {
            polling: Duration::from_millis(10),
            start_timeout: Duration::from_millis(25),
            frame_capacity_bytes: 4096,
            transport_timeout: Duration::from_millis(40),
            subscriber_capacity: 8,
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
    fn successful_close_delivers_one_dead_only_to_current_subscribers() {
        use signal::FrameReading;
        use signal_capsule_nexus::{Outcome, Input, Output, State};
        let (dispatcher, _emitter, pane) = started_dispatcher(Vec::new());
        let configuration = test_configuration();
        let (server, mut client) = std::os::unix::net::UnixStream::pair().expect("pair");
        client.set_read_timeout(Some(Duration::from_millis(150))).expect("timeout");
        dispatcher.subscribe(server, pane.session.clone(), Arc::clone(&configuration)).expect("subscribe");
        let _ = read_subscription(&mut client);
        let _ = read_subscription(&mut client);
        assert!(matches!(
            dispatcher.dispatch(Input::Close(pane.clone()), Arc::clone(&configuration)),
            Output::Closed(Outcome::Done)
        ));
        assert!(matches!(
            read_subscription(&mut client),
            capsule_nexus_wire::public_call::Output::Answered(ref output)
                if matches!(&output.0, Output::Changed(change) if change.pane == pane && change.state == State::Dead)
        ));
        client.set_read_timeout(Some(Duration::from_millis(40))).expect("short timeout");
        assert!(client.read_frame(signal::FrameCapacity::from(4096)).is_err());

        let (late_server, mut late_client) = std::os::unix::net::UnixStream::pair().expect("late pair");
        late_client.set_read_timeout(Some(Duration::from_millis(40))).expect("late timeout");
        dispatcher.subscribe(late_server, pane.session, configuration).expect("late subscribe");
        let _ = read_subscription(&mut late_client);
        assert!(late_client.read_frame(signal::FrameCapacity::from(4096)).is_err());
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
        let (server, mut client) = std::os::unix::net::UnixStream::pair().expect("pair");
        client
            .set_read_timeout(Some(Duration::from_millis(150)))
            .expect("timeout");
        dispatcher
            .subscribe(server, pane.session.clone(), Arc::clone(&configuration))
            .expect("subscribe");
        let _ = read_subscription(&mut client);
        let _ = read_subscription(&mut client);

        assert!(matches!(
            dispatcher.dispatch(Input::Close(pane.clone()), Arc::clone(&configuration)),
            Output::Closed(Outcome::Done)
        ));
        assert!(matches!(
            read_subscription(&mut client),
            capsule_nexus_wire::public_call::Output::Answered(ref output)
                if matches!(&output.0, Output::Changed(change) if change.pane == pane && change.state == State::Dead)
        ));
        assert!(matches!(
            dispatcher.dispatch(Input::List(pane.session), configuration),
            Output::Listed(Outcome::Listed(ref known)) if known.is_empty()
        ));
    }

    #[test]
    fn another_close_refusal_retains_ownership_and_publishes_no_dead() {
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
        let (server, mut client) = std::os::unix::net::UnixStream::pair().expect("pair");
        client
            .set_read_timeout(Some(Duration::from_millis(80)))
            .expect("timeout");
        dispatcher
            .subscribe(server, pane.session.clone(), Arc::clone(&configuration))
            .expect("subscribe");
        let _ = read_subscription(&mut client);
        let _ = read_subscription(&mut client);

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
        use signal::FrameReading;
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
        let dispatcher = Arc::new(Dispatcher::with_emitter(&test_paths(), emitter));
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

        let (server, mut client) = std::os::unix::net::UnixStream::pair().expect("pair");
        client.set_read_timeout(Some(Duration::from_millis(250))).expect("timeout");
        dispatcher
            .subscribe(server, pane.session.clone(), Arc::clone(&configuration))
            .expect("subscribe");
        let _ = read_subscription(&mut client); // acknowledgement
        let _ = read_subscription(&mut client); // current Busy
        poll_started_rx.recv().expect("old poll reached external List");

        assert!(matches!(
            dispatcher.dispatch(Input::Close(pane.clone()), Arc::clone(&configuration)),
            Output::Closed(Outcome::Done)
        ));
        assert!(matches!(
            read_subscription(&mut client),
            capsule_nexus_wire::public_call::Output::Answered(ref output)
                if matches!(&output.0, Output::Changed(change) if change.pane == pane && change.state == State::Dead)
        ));
        release_poll_tx.send(()).expect("release stale poll");

        client.set_read_timeout(Some(Duration::from_millis(80))).expect("short timeout");
        assert!(
            client.read_frame(signal::FrameCapacity::from(4096)).is_err(),
            "the stale pre-Close List must not publish Open after terminal Dead"
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
        use capsule_nexus_wire::exchange::{Patience, QueryingConnection};
        use capsule_nexus_wire::public_call::{Input, Output};
        use signal::Delivery;
        let (dispatcher, _emitter, pane) = started_dispatcher(Vec::new());
        let (server, client) = std::os::unix::net::UnixStream::pair().expect("pair");
        let caller = capsule_nexus_wire::frame::peer_caller(&client).expect("caller");
        let connection = PublicConnection::new(server, PublicAuthority::Translator).expect("connection");
        let serving = Arc::clone(&dispatcher);
        std::thread::spawn(move || connection.serve(&serving, &test_configuration()));
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
        let connection = PublicConnection::new(server, PublicAuthority::Translator).expect("connection");
        let serving = Arc::clone(&dispatcher);
        std::thread::spawn(move || connection.serve(&serving, &test_configuration()));
        let capacity = signal::FrameCapacity::from(4096);
        capsule_nexus_wire::exchange::write_value(
            &client,
            &Dispatch::Open(Opening { exchange: signal::FIRST_EXCHANGE, query: public_query("any", &caller) }),
            Duration::from_millis(500),
            capacity,
        )
        .expect("open");
        let mut reading = client.try_clone().expect("reading");
        reading.set_read_timeout(Some(Duration::from_millis(500))).expect("timeout");
        let body = capsule_nexus_wire::exchange::read_frame_or_end(&mut reading, capacity)
            .expect("frame")
            .expect("an ending");
        assert_eq!(
            capsule_nexus_wire::exchange::restored::<Delivery<capsule_nexus_wire::public_call::Output>>(body)
                .expect("delivery"),
            Delivery::End(Ending::connection_faulted(ExchangeFault::GreetingExpected))
        );
        assert!(capsule_nexus_wire::exchange::read_frame_or_end(&mut reading, capacity)
            .expect("the end of the connection")
            .is_none());
    }

    #[test]
    fn a_lagged_subscription_ends_its_exchange_and_a_withdrawn_one_its_connection() {
        use signal::{Delivery, Ending, ExchangeFault, ExchangeTracking, Greeted};
        let (server, client) = std::os::unix::net::UnixStream::pair().expect("pair");
        let connection = PublicConnection::new(server, PublicAuthority::Translator).expect("connection");
        let lagged = signal::FIRST_EXCHANGE;
        let withdrawn = signal::ExchangeId(2);
        {
            let mut ledger = connection.ledger.lock().expect("ledger");
            ledger.greet().expect("greeted");
            ledger.admit(lagged).expect("lagged admitted");
            ledger.admit(withdrawn).expect("withdrawn admitted");
        }
        connection.conclude(lagged, Closing::Lagged, Some(test_configuration()));
        let capacity = signal::FrameCapacity::from(4096);
        let mut reading = client.try_clone().expect("reading");
        reading.set_read_timeout(Some(Duration::from_millis(500))).expect("timeout");
        let body = capsule_nexus_wire::exchange::read_frame_or_end(&mut reading, capacity)
            .expect("frame")
            .expect("an ending");
        assert_eq!(
            capsule_nexus_wire::exchange::restored::<Delivery<capsule_nexus_wire::public_call::Output>>(body)
                .expect("delivery"),
            Delivery::End(Ending::faulted(lagged, ExchangeFault::Lagged))
        );
        connection.conclude(withdrawn, Closing::Withdrawn, Some(test_configuration()));
        assert!(capsule_nexus_wire::exchange::read_frame_or_end(&mut reading, capacity)
            .expect("the end of the connection")
            .is_none());
    }

    #[test]
    fn bounded_subscriber_queue_disconnects_only_the_overflowing_peer() {
        use signal_capsule_nexus::{Outcome, Output};
        let slow = SubscriberQueue::new(1);
        let healthy = SubscriberQueue::new(2);
        assert!(slow.enqueue(Output::Subscribed(Outcome::Done)));
        assert!(!slow.enqueue(Output::Subscribed(Outcome::Done)));
        assert!(!slow.is_open());
        assert!(healthy.enqueue(Output::Subscribed(Outcome::Done)));
        assert!(healthy.is_open());
    }

    #[test]
    fn lowering_subscriber_capacity_disconnects_only_queues_above_the_new_bound() {
        use signal_capsule_nexus::{Outcome, Output};
        let store = ConfigurationStore::default();
        let over = Arc::new(SubscriberQueue::new(3));
        let within = Arc::new(SubscriberQueue::new(3));
        store.register_subscriber(&over);
        store.register_subscriber(&within);
        assert!(over.enqueue(Output::Subscribed(Outcome::Done)));
        assert!(over.enqueue(Output::Subscribed(Outcome::Done)));
        assert!(within.enqueue(Output::Subscribed(Outcome::Done)));
        store.apply_subscriber_capacity(1);
        assert!(!over.is_open());
        assert!(within.is_open());
    }
}
    #[test]
    fn core_accepts_only_the_paired_translator_process() {
        use shared_translator::{PeerIdentity, ProcessId, UserId};
        let (client, _server) = std::os::unix::net::UnixStream::pair().expect("socket pair");
        let actual = capsule_nexus_wire::frame::peer_caller(&client).expect("peer credentials");
        let paired = BoundPeer::capture(PeerIdentity {
            user_id: actual.user_id,
            process_id: actual.process_id,
        }).expect("capture current process");
        assert!(require_peer(&client, &paired).is_ok());
        let forged = BoundPeer::capture(PeerIdentity {
            user_id: UserId(actual.user_id.0),
            process_id: ProcessId(actual.process_id.0),
        }).expect("capture current process");
        let mismatched = shared_translator::Caller {
            process_id: ProcessId(actual.process_id.0 + 1),
            ..actual
        };
        assert!(!forged.matches(&mismatched));
    }
