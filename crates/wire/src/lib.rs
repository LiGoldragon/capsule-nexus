//! The Capsule Nexus's own wire: its call contracts, its bootstrap frames and
//! its name. The exchange layer, the lanes and the admission they share are
//! every Nexus's, in `nexus-exchange`.

pub mod exchange;
pub mod frame;
#[allow(unused_imports)] // the generated module imports the transport types it may not use
pub mod meta_call;
pub mod meta_payload;
#[allow(unused_imports)] // the generated module imports the transport types it may not use
pub mod public_call;
pub mod public_payload;
pub mod transport_types;

pub use nexus_exchange::ValidAdmission;

use nexus_exchange::{ComponentName, ComponentTitle, Named, SocketNaming};

/// The Capsule Nexus, which its sockets are named after.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CapsuleNexus;

impl Named for CapsuleNexus {
    fn name() -> ComponentName {
        ComponentName("capsule".into())
    }

    fn title() -> ComponentTitle {
        ComponentTitle("Capsule Nexus".into())
    }
}

pub fn socket_names() -> nexus_exchange::SocketNames {
    CapsuleNexus::name().socket_names()
}
