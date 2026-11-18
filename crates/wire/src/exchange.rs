//! Both Capsule Nexus sockets speak `signal`'s exchange layer, which
//! `nexus-exchange` frames and `nexus-serving` serves.
//!
//! The query of every exchange is the Call with its Caller:
//! `public_call::Input` on the public socket, `meta_call::Input` on the meta
//! socket. Each greets with the digest of its own authored source.

use signal::Contracted;

/// The authored source of the public call contract, whose digest greets the
/// public socket.
pub const PUBLIC_CALL_ETHOS: &str = include_str!("../ethos/public-call.ethos");

/// The authored source of the meta call contract, whose digest greets the
/// meta socket.
pub const META_CALL_ETHOS: &str = include_str!("../ethos/meta-call.ethos");

impl Contracted for crate::public_call::Input {
    const CONTRACT_SOURCE: &'static str = PUBLIC_CALL_ETHOS;
}

impl Contracted for crate::meta_call::Input {
    const CONTRACT_SOURCE: &'static str = META_CALL_ETHOS;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_two_call_contracts_greet_with_different_digests() {
        assert_ne!(
            crate::public_call::Input::contract_digest(),
            crate::meta_call::Input::contract_digest()
        );
    }

    #[test]
    fn sockets_are_named_after_the_capsule_nexus() {
        let names = crate::socket_names();
        assert_eq!(names.public_nexus_socket.0, "capsule.sock");
        assert_eq!(names.meta_nexus_socket.0, "capsule-meta.sock");
        assert_eq!(names.public_translator_socket.0, "capsule-translator.sock");
        assert_eq!(
            names.meta_translator_socket.0,
            "capsule-meta-translator.sock"
        );
    }
}
