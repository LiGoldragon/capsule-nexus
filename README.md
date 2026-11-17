# capsule-nexus

Branch `rename/capsule-cdc93e` holds the approved Capsule Nexus naming migration; flow `cdc93e` owns it. Deployment remains unchanged.

Version 0 proof of the Nexus that owns herdr panes. The public, meta, and
Emission contracts are authored in Ethos and generated without hand-written
wire types. The pure core owns pane Memory and outcome transitions; the
translator owns Datom, JSON, and process execution.

`ethos/emission.ethos` is the documented temporary Signal-root stand-in for
Emission. Ethos-zero is unchanged.

The current `InMemory` backend is explicitly nondurable and provides no
restart recovery. A Subscribe is answered by a stream: `Opened` with the
session's panes that are not Dead, whoever owns them, then each pane that
moved once as `Changed`, the pane whole with its label and attempt, and `Lagged` with the panes anew for a subscriber that fell behind,
which goes on from them. All subscribers to one session share one Publisher and
one feed; successful disappearance tells `Dead` once; refused or uncertain
observations preserve the last verified state. A Configure of the subscriber
capacity resizes every session's Publisher, and each open stream follows it. A
disconnected reader subscribes again and is given the state.

Build the full check through Nix:

```sh
nix build .#checks.x86_64-linux.test --option max-jobs 0 -L
```
