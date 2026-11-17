# Upgrades

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
