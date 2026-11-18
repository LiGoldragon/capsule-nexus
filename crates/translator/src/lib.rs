//! The Capsule Nexus's translator: its own settings and the herdr calls it
//! makes for the Nexus, the bodies it writes for the serving every
//! translator shares (`translator-serving`), and its CLIs on the client end
//! (`translator-client`).

pub mod runner;
pub mod server;
pub mod generated {
    pub mod argument;
}
pub use nexus_exchange::lane as transport;
pub use server::CapsuleTranslator;
