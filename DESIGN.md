# Capsule Nexus version 1 proof

## Boundaries

The Nexus core receives and returns generated binary Signal values. It has no
Datom, JSON, command-line, or process dependency. The translator owns the
external herdr JSON and command line. `ethos/emission.ethos` is temporarily a
Signal root because ethos-zero does not yet check an Emission root.

Ethos-zero emits the Rust root names `Input` and `Output` for a Signal, as the
vision calls them, with its `Answering` and `Answerable` traits beside them.

## Memory

`InMemory` is a process-lifetime proof backend. It is nondurable and offers no
restart recovery. `ethos/memory.ethos` declares the owned-pane and
reconciliation records and the standard `Answer` returned by every change;
the pinned generator output is checked in every Nix test run. Reads return
typed snapshots separately. The `Memory` trait is the persistence boundary. Tests use a
deterministic `RefusingMemory` to prove three facts separately: refusal before
an external attempt, an observed external success followed by Memory refusal,
and uncertainty whose reconciliation marker could not be stored.

A pane is acted on only after Memory proves ownership. Closing a pane closes
that pane only. A successful complete list may mark an absent owned pane dead;
a refusal or uncertainty changes no pane to dead. Labels are stored unchanged
in Memory and joined onto public observations there; external observations do
not carry labels.

Each public `Known` carries `State` and `Attempt` as separate facts. `State`
describes the observed pane; `Attempt.Settled` or `Attempt.Uncertain` describes
the one bounded record of the latest attempt. An uncertain attempt therefore
does not invent a pane state, and a later successful reconciliation settles
the attempt independently of the observed state it supplies.

Create is reconciled before it has a pane identity. The launcher gives each
core process a fresh random `InstanceId`; the core pairs it with a monotonic
`Sequence` and stores the resulting `PendingCreation` before emitting. The
translator writes that identifier into the one managed workspace label. A
lost Create result leaves the single pending record actionable. A later
complete workspace snapshot and the same observation cycle's complete pane
snapshot adopt only an exact one-workspace, one-pane join, then consume the
pending record. Ambiguous, malformed, absent, and unrelated observations
leave it pending. This proof Memory remains nondurable, so restart recovery is
outside the proof's guarantee.

## Outcomes repaired from the draft

The vision draft used one broad `Outcome` and did not express the point where
an external effect succeeded but its Memory write failed. The checked contract
keeps one shared `Outcome` and adds `StoreRefused` with a phase:

- `BeforeEmission` — Memory refused before an external attempt.
- `Observed` — the exact successful effect is carried as evidence.
- `Uncertain` — the external effect may have happened and its reconciliation
  marker could not be stored.

Symbolic herdr error codes remain strings. An optional process exit status is
separate. Internal failures never receive invented exit codes.

## Configuration and placement

The Nexus and translator each own a partial configuration. One flat typed
`Set` validates and applies one field; refusal preserves the previous partial
snapshot. The Nexus owns start, polling, frame-capacity, transport and
subscriber-capacity limits; the subscriber capacity is the capacity a session's
Publisher is made with, held by the Capsule actor alone: a Configure of it sets
the actor's capacity and resizes every session's Publisher in the actor's turn,
each open stream following it, so a Subscribe that read its configuration
earlier still opens at the capacity held now. One more than the capacity behind
is handed `Lagged` with the panes as they stand and goes on.
The translator owns the herdr path, call timeout, output limit and text budget.
Each process reports the fields it still awaits and becomes ready only when
all of its fields are present and their cross-field constraints hold. Defaults
are deployment data, not Rust literals. Each attempt captures the fields it
uses without holding Memory across external I/O.

A launcher selects one `Place` from its typed registry and supplies exactly one
typed binary bootstrap carrying the place and admission limits. Admission
includes a positive deployment-chosen accept retry delay: recoverable resource
pressure waits by that delay, while an invalid listener ends the owning
process visibly. Each metaflow
has separate public and meta translator sockets and separate public and meta
Nexus sockets. The selected place is immutable for each bound process. The CLI
takes a metaflow name and the launcher-supplied
registry path; it does not discover sockets through an environment variable or
a hard-coded home directory.

The meta surfaces are admitted under bootstrap limits before runtime settings
exist. Public service becomes ready only after the owning process has a
complete valid configuration. The public text and emission lanes share the
public translator socket; meta text has its own translator socket. Every lane
checks the peer UID at the same-login boundary.

## The Nexus sockets speak exchanges

Both Nexus sockets, public and meta, speak `signal`'s exchange layer. The
querying side greets a connection once with the digest of its call contract's
authored source (`crates/wire/ethos/public-call.ethos` or `meta-call.ethos`);
a peer built from another source is refused and the connection closes. After
the greeting the connection carries any number of exchanges, each opened by the
querying side with an identifier it mints and a query that is the Call with its
Caller: `public_call::Input` or `meta_call::Input`, the envelope and the
identity as they were. Every frame the Nexus sends names its exchange.

A one-answer input is answered by one `Delivery::Answer` on its exchange, and
the exchange ends with it. A Subscribe is an exchange that goes on answering,
one Answer per frame of its stream, `Subscribed.Stream<Panes Known>`:
`Opened` first, with the panes the Capsule holds in the session that are not
Dead, whoever owns them; then each pane that moved as `Changed`, its `Known`
whole; and `Lagged` with the panes anew
for a subscriber that fell more than the subscriber capacity behind, which goes
on from them. A refused Subscribe is answered once, `SubscribeRefused`.
Observing is open; acting stays the owner's. A subscription withdrawn with its
peer's binding closes its connection; the querying side unsubscribes by
abandoning the exchange or closing the connection.

The Capsule's state is owned by one Kameo actor on the daemon's tokio runtime:
Memory, ownership, reservations and revisions, and for each session that has a
subscriber one `nexus::Publisher` over its panes, which holds the published
state once. Each step an attempt makes on that state is one message; herdr calls
run between the steps on the runtime's blocking pool. The operation that
changes Memory publishes the pane's `Known` whole when Memory answers
`Succeeded` and the Known moved, its label, state or attempt, so a Start's label
reaches every open stream and a pane is told Dead once and leaves the state. The
Publisher is made at a session's first Subscribe and dropped with its last
stream. While it lives, the session's feed observes the session with List's
herdr calls once per polling interval and reconciles what it sees into Memory,
whose changes are published; the feed ends when the Publisher is dropped.

The public socket answers each exchange on a task of its own, so several
calls and subscriptions share one connection; a subscription's task is woken by
its stream's next frame or by its closing, never by a timer; the authority is the
connection's, taken from its peer credentials at accept, and each Call's Caller
is checked against it. The greeting must arrive within the transport timeout;
after it a connection may be quiet. The meta socket serves one connection at a
time, the whole of it within the bootstrap deadline, and answers its exchanges
in turn.

## External calls

Each external command bears the checked `Arguable` trait on its own concrete
argument type. The one typed execution path composes that capability with the
one session prefix and decodes through the same concrete command type; there
is no parallel invocation or decoder selection table.
Successful Herdr JSON is decoded only from stdout. A nonzero process result is
decoded only from Herdr's documented stderr error envelope, with no fallback
between the channels. The bounded runner drains both pipes while retaining at
most the configured joint output limit. `Subscribe` is routed once into its
dedicated stream, one exchange on the Nexus; it has no parallel one-shot fallback.

Every authored Ethos contract has its own deterministic generated-file check.
The aggregate `generated` check depends on all of those checks; it does not
stand in for any individual comparison.

## External state

The translator maps herdr status to public state as follows: positively
detected readiness is `Idle`; working or blocked is `Busy`; a present pane
without positive readiness is `Open`; absence becomes `Dead` only after a
successful complete snapshot. Failed, malformed, partial, or timed-out listing
is `Refused` or `Uncertain` and never a complete empty snapshot.
