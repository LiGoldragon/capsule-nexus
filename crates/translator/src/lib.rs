//! The Capsule Nexus's translator: its own settings and the herdr calls it
//! makes for the Nexus, the bodies it writes for the serving every
//! translator shares (`translator-serving`), and its CLIs on the client end
//! (`translator-client`).

pub mod instance;
pub mod runner;
pub mod server;
pub mod generated {
    pub mod argument;
}
pub use capsule_nexus_wire::transport_types;
pub use nexus_exchange::lane as transport;
pub use server::CapsuleTranslator;
