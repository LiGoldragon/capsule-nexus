# Upgrades

## 0.9.0 — the shared layer's 0.2.0: every exchange ends, a CLI takes its datom alone, one Nexus bootstrap, a typed lane failure

The translator and the wire move to 0.9.0, the daemon to 0.8.0 and emission
to 0.5.0; core and core-meta stay.

**Pins.** The nexus crates 0.2.0 at fb3441f and `translator-client` and
`translator-serving` 0.2.0 at 0b44de7; build-base registers them
(82015ff).
translator 0.5.2 at 29601a3, nexus 0.9.0, signal 11.2.0 and both contracts
stay.

**Every exchange ends.** The Nexus ends a one-answer exchange Completed
after its answer, as it ends a subscription; the translator writes each
answer back until the end and then `Ended`, or `Failed` with what ended it,
so a refused Subscribe (`SubscribeRefused`) is answered once and ends, and
the CLI exits after printing it. `PublicTranslating::answers` is gone: the
translator no longer tells a Subscribe from any other query.

**A CLI takes its datom alone.** `capsule '<datom>'` and `capsule-meta
'<datom>'`; the place is the one `$XDG_CONFIG_HOME/nexus/capsule.datom`
names (`~/.config/nexus/capsule.datom` when the variable is unset), `{
<place-name> }`, resolved under `$XDG_RUNTIME_DIR/nexus/`. A configuration
missing, unreadable or invalid is refused naming the file,
`Missing.<path>`, `Unreadable.{ <path> <reason> }` or `Invalid.{ <path>
<reason> }`, exit 2, and nothing is sent; a second argument is a usage
refusal, exit 2.

**One Nexus bootstrap.** `CoreBootstrap` and `crates/wire/ethos/transport.ethos`
are gone, with the wire's `transport_types`: the daemon reads
`nexus_serving::NexusBootstrap`, the same four fields in the same archive,
and the pair launcher in `translator-serving` writes it with a fresh
instance identifier, so `PairLaunching` and the translator crate's
`instance` module are gone. `capsule-place … Core` writes a
`NexusBootstrap`.

**The text budget starts at the bootstrap's.** `CapsuleTranslator` is made
from its `TranslatorBootstrap` (`TryFrom`), its text budget the bootstrap's
text limits, which the launcher takes from the flake's pairing values; the
meta socket's `Configure.TextBudget` sets it after, and the translator's
readiness no longer awaits it. `CapsuleTranslator::default()` is gone; a
test makes one `From` a `ValidTextBudget`.

**The translator's settings in a forwarded readout.** A Nexus setting's
`Configured` readiness carries the translator's own settings still awaited,
first, then the Nexus's: `Ready` only when both are. The meta contract has
no subscription, so the translator announces nothing (`announced` is
`None`).

**A failed lane is typed.** `emission.ethos`'s output gains
`LaneFailed.nexus_exchange:LaneFailure`, and `emission_capsule_nexus::Output`
bears `LaneFailing`. The daemon reads an unsubmitted or declined lane as the
translator unavailable and an unanswered one as uncertain; a lane fault never
reads as an outcome the world answered.

**To adopt.** Write each CLI's place into `nexus/capsule.datom` under the
configuration home, and drop the place argument from every call.

## 0.8.0 — the translator is every translator's

The translator and the wire move to 0.8.0; core, core-meta, daemon and
emission stay.

**Pins.** `translator-client` and `translator-serving` 0.1.0, one revision
of the translator repository (fc03795); build-base registers them
(f00b376). translator 0.5.2 at 29601a3, nexus 0.9.0, the nexus crates at
8976179, signal 11.2.0 and both contracts stay.

**What moved.** `TextClient` and its lanes leave the translator crate for
`translator-client`; the translator sockets, the per-connection serving, the
forwarding of a public Call once or followed and of a meta Call within one
deadline, the emission lane, the text budget, reading the bootstrap and the
pair launcher leave it for `translator-serving`. `TranslatorBootstrap`
leaves `transport.ethos` for `translator-serving`'s Ethos: its text limits
are `TextLimits.{ NodeLimit DepthLimit }`, the archive the old
`BudgetValue` field had, so the frame is the same bytes. The wire keeps
`CoreBootstrap` and no longer depends on the meta contract. The translator
crate keeps its own: `CapsuleTranslator` implementing `Translator`,
`PublicTranslating`, `MetaTranslating`, `EmissionAnswering` and
`PairLaunching`, its settings, the herdr runner and decoders, and the five
binaries, each a few lines over the shared crates. Its `text` feature turns
on `translator-serving/text`. Removed from the translator crate's library:
`TextClient`, `ClientError`, `text_budget`, `maximum_text_budget`, the
`placement` re-export, `server::run`, `server::serve_one`, `Endpoint` and
`ServerError`; use the shared crates' `TextClient`, `ValidTextBudget`,
`TranslatorServing`, `Connection` and `TranslatorError`.

**Behaviour.** Unchanged: the sockets, the lanes, the exchange frames, the
bootstrap frames' archives, the launcher's arguments and every Datom text
are as they were. Client-facing failure text names no component: an
exchange error the translator answers with reads `Nexus exchange error`
where it read `Capsule Nexus exchange error`, a CLI's error reads
`translator client error` where it read `herdr translator client error`,
and the launcher ends with `paired process ended`, where it said `paired
Herdr process ended`, and names a missing `NEXUS_EXECUTABLE` where it named
`CORE_EXECUTABLE`. The translator's stderr lines name it by its title,
`Capsule Nexus translator`.

To deploy: rebuild the daemon, the translator and the CLIs together.

## 0.7.0 — the serving is every Nexus's

wire, daemon and the translator move to 0.7.0; core, core-meta and emission
stay.

**Pins.** `nexus-exchange`, `nexus-querying` and `nexus-serving` 0.1.0, one
revision of the nexus repository; build-base registers them. ethos-zero
30.0.1 (`2999292`) generates and checks the contracts; every committed
generation is unchanged by it. nexus 0.9.0, signal 11.2.0, translator 0.5.2
and both contracts stay.

**What moved.** The exchange helpers, `QueryingConnection`, the lane framing,
`DeadlineStream`, the accept retry and `ValidAdmission` leave the wire crate
for `nexus-exchange` and `nexus-querying`; `Lane`, `Kind`, `SocketNames` and
`Admission` with its three integers leave `transport.ethos` for
`nexus-exchange`'s Ethos, and `transport.ethos` keeps the two bootstrap
frames. `OwnedListener`, `SocketPaths`, the meta-then-public bootstrap,
paired-peer admission, the meta exchange loop, `PublicConnection`, the
subscription closing and the emission lane leave the daemon for
`nexus-serving`. The wire keeps the call contracts, the bootstrap frames and
`CapsuleNexus`, whose name gives the four sockets; the daemon keeps its
configuration, its actor and its bodies: `MetaAnswering` for `Bootstrap`,
`PublicServing` for `Dispatcher`, `PeerAdmitting` for `ConfigurationStore`,
`TransportBounded` for `ReadyCoreConfiguration`. `SocketPaths`,
`AcceptedPublic` and `PublicConnection` are the shared types for the Capsule.

**Behaviour.** Unchanged: the sockets, the exchange layer's frames, the lane
header, the bootstrap frames' archives and every Datom text are as they
were. Only diagnostics on stderr change, where a transport or exchange error
displays as `Nexus transport error` or `Nexus exchange error`; an exchange
error the translator answers with still reads `Capsule Nexus exchange error`.

To deploy: rebuild the daemon and the translator together.

## 0.6.0 — each pane moved is published whole; the subscriber capacity resizes

core, daemon, wire and the translator move to 0.6.0; core-meta and emission
stay at 0.4.0.

**Pins.** signal-capsule-nexus 3.0.0 (`c019`), nexus 0.9.0 (`d020`); build-base
registers them. signal 11.2.0, translator 0.5.2 and meta-signal-capsule-nexus
1.0.1 stay.

**The wire.** A Subscribe's stream is `Subscribed.Stream<Panes Known>`: each
`Changed` carries the pane's `Known` whole, its label and attempt with its
state, so a label given at Start reaches every open stream. A pane is published
whenever its Known moved: a state, a label, or an attempt made Uncertain or
settled. A Known whose state is Dead takes the pane out.

**Configure.** A `SubscriberCapacity` Configure resizes every session's
Publisher in the Capsule actor's turn (`nexus::Publishing::resize`); each open
stream follows the new capacity, one further behind than it is handed `Lagged`
with the panes as they stand, then goes on exact.

## 0.5.0 — Subscribe is answered by a stream from a Publisher

core, daemon, wire and the translator move to 0.5.0; core-meta and emission
stay at 0.4.0.

**Pins.** signal-capsule-nexus 2.0.0 (`49b4`), nexus 0.8.0 (`4765`), kameo
0.22.2 and tokio 1.53.1; build-base `79c4` registers them. signal 11.2.0,
translator 0.5.2 and meta-signal-capsule-nexus 1.0.1 stay.

**The wire.** A Subscribe's answers on its exchange are the frames of
`Subscribed.Stream<Panes Changed>`, each `Output::Subscribed(Frame)`:
`Opened` with the panes the Capsule holds in the session that are not Dead,
whoever owns them; each `Changed.{ Pane State }`; and `Lagged` with the panes
anew for a subscriber more than the subscriber capacity behind, which goes on
from them. There is no acknowledgement frame and no `Output::Changed`; a
refused Subscribe is answered once, `SubscribeRefused.Outcome`. Nothing ends an
exchange `Faulted.Lagged` any more.

**Who may subscribe.** Any admitted caller: the subscription is one for the
session, over every pane in it, whoever owns it. Acting on a pane stays its
owner's.

**The daemon.** The Capsule's state is owned by one Kameo actor, `Capsule`, on
a tokio runtime the daemon starts; the public socket is served on that runtime,
each exchange on a task, its attempt on the blocking pool. Each session with a
subscriber has one `nexus::Publisher` in the Core, made at its first Subscribe
and dropped with its last stream; the operation that changes Memory publishes
the change when Memory answers `Succeeded`. The subscriber queues and writer
threads are gone, and with them the per-subscriber change diffing: a
subscription's task is woken by its stream or its closing. The List-timer feed
stays, one per session while its Publisher lives, at the polling interval as
before, and reaches the Publisher through Memory.

**Operation.** `Subscribing` and `Subscribed` leave `ethos/operation.ethos`: a
subscription opens on the Publisher and changes nothing in Memory.

**Configuration.** `SubscriberCapacity` is the capacity a session's Publisher
is made with; a Publisher already made keeps its capacity until it is dropped.

To deploy: read each Subscribe answer as a frame of `Output::Subscribed`, and
redeploy the daemon and translator together.

## 0.4.0 — the Nexus sockets speak signal's exchange layer

All six packages move to 0.4.0. What a user sees is unchanged: the CLI, the
translator's text and emission lanes and every Datom text are as they were.

**Pins.** signal 11.2.0 (`4f5c`), translator 0.5.2 (`2960`),
signal-capsule-nexus 1.0.1 (`d57c`) and meta-signal-capsule-nexus 1.0.1
(`76b4`), each only following signal; build-base `982f` registers them.

**The wire of both Nexus sockets.** A frame was one bare archive, one Call in
and its Output back, one call per connection, a subscription's Outputs following
until the connection closed. A frame is now one archive of the exchange layer:

- the querying side sends `signal::Dispatch<public_call::Input>` on the public
  socket and `signal::Dispatch<meta_call::Input>` on the meta socket: first
  `Greet` with the call contract's digest, then `Open` with an exchange
  identifier and the Call with its Caller, and `Abandon` to give one up;
- the Nexus sends `signal::Delivery<public_call::Output>` or
  `signal::Delivery<meta_call::Output>`: `Greeted` with its receipt, `Answer`
  with the exchange and the Output, and `End` with the exchange and its
  conclusion;
- one connection carries any number of exchanges; a Subscribe answers its
  acknowledgement, the current state and each change on its one exchange; a
  subscriber whose queue overflows has its exchange ended `Faulted.Lagged`
  where its connection used to close;
- a frame the Nexus cannot read, an exchange opened before the greeting and a
  second greeting are each answered by `End` against the connection, which
  then closes.

The Call, its Caller, both payloads, the transport types and the Emission
contract are unchanged. `crate::exchange` holds the call contracts' digests
(`Contracted` for `public_call::Input` and `meta_call::Input`), the querying
connection and the two frame reads.

**Ethos.** The wire crate's declarations move with it, unchanged, from
`ethos/` to `crates/wire/ethos/`, so the crate reads the call contracts'
sources for their digests.

**API.** `serve_public_once` and `serve_meta_once` are gone:
`PublicConnection::new(stream, authority)` and `PublicConnection::serve` serve
a public connection, and `serve_meta_connection` a meta connection.
`Dispatcher::subscribe_bound` takes the connection and the exchange where it
took a stream; `Dispatcher::subscribe` and `Dispatcher::subscribe_as` are gone.

## 0.3.0 — ethos-zero 30.0.0 and signal 11.1.0

All six packages move to 0.3.0. Behaviour is unchanged.

**Pins.** signal-capsule-nexus 1.0.0 (`537f`), meta-signal-capsule-nexus 1.0.0 (`0a9e`), translator 0.5.1 (`2310`) and signal 11.1.0 (`b145`); datom-codec 09a0526 and protos c93b598 stay. The generator is ethos-zero 30.0.0 (`b97d`).

**Generated names.** Every Signal's roots are `Input` and `Output` where they were `Query` and `Response`, here and in both contracts, and each generates `Answering` and `Answerable` beside them. `Outcome::AttemptInProgress` carries `Pane` directly.

**Ethos.** A Signal may no longer import names `Input` and `Output`, so `public-call.ethos` and `meta-call.ethos` import their payloads as `PublicInput`/`PublicOutput` and `MetaInput`/`MetaOutput`, and `Call_Data`'s field is `public_input` or `meta_input` where it was `input`. The payload Libraries name `signal_capsule_nexus:Input`/`Output` and `core_meta_capsule_nexus:Input`/`Output` and import nothing. Variant names, order and payloads are unchanged, so the rkyv archive and the Datom text of every frame are unchanged.

## 0.2.0 — Protos 0.33.0 and the nexus place layout

All six packages move to 0.2.0.

**Pins.** signal-capsule-nexus 0.2.0 (`7c72`), meta-signal-capsule-nexus 0.2.1 (`5b89`), translator 0.3.1 (`1b0e`), signal 10.3.0 (`7afb`), datom-codec 0.33.0 (`eabe`) and protos 0.33.0 (`3c7a`); the generator is ethos-zero 19.2.0 (`5a5d`). The lock holds one source of each. The generated emission and transport projections add derives only. `ReaderBudget` is built with `try_from`.

**Where a place lives.** The translator this body used before, `3a43`, named the place `MetaflowName` and resolved its runtime and state below `metaflow/<name>`; the translator it now selects, `1b0e` (0.3.1, in the line that began with translator 0.2.0), names it `PlaceName` and resolves below `nexus/<name>`. The sandboxed checks and fixtures use the `nexus` layout.

**Automatic place preparation.** The launcher (`capsule-pair`) and the direct daemon (`capsule-nexus-daemon`) both call translator's `prepare_for_start` on the place they are given: the launcher after validating the place and before its directory checks and before it starts either child, the daemon before it opens any socket or state. For a standard place, whose state is `<root>/nexus/<name>`, this

- moves a sole legacy state directory `<root>/metaflow/<name>` to `<root>/nexus/<name>` with a single rename, keeping every byte;
- creates the runtime and state directories that are missing, at mode 0700;
- takes one exclusive lock per place, `<root>/nexus-place-locks/<name>`, so two starts of the same place serialize; the lock file stays;
- refuses to start, and changes nothing, when both `<root>/metaflow/<name>` and `<root>/nexus/<name>` exist. Resolve it by hand: keep one directory, set the other aside, and start again. The refusal names both paths.

If the move succeeds and creating the runtime then fails, the state is already in place; start again after fixing the cause, and nothing is moved twice. A symlink, a directory owned by another user, or one writable by group or others in place of a place directory is refused; mode 0755 directories created by earlier versions are accepted. A place with an explicit state path outside `<root>/nexus/<name>` only has its directories created and is never migrated.

A failed preparation stops the launcher or the daemon before any child is started or socket opened. The exclusion serializes the preparation step of one place, from the existence checks to the end of the directory creations; it is released before the launcher starts its children and the daemon opens its sockets, so it does not serialize whole starts, does not own the place while it runs and does not stop other writers.

**Operators.** Nothing is moved until a build with this change starts a place. A host that still has state under `metaflow/<name>` gets it moved at that start; a host already on `nexus/<name>` is unchanged. The name `main` under the shared `nexus/` directory may belong to another nexus on a host; use a distinct place name.
