# Upgrades

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
