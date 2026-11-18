//! The Capsule Nexus's own wire: its call contracts and its name. The exchange
//! layer, the lanes and the admission they share are every Nexus's, in
//! `nexus-exchange`, and the bootstrap a Nexus starts with is every Nexus's,
//! in `nexus-serving`.

pub mod exchange;
pub mod frame;
pub mod meta_call;
pub mod meta_payload;
pub mod public_call;
pub mod public_payload;

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
