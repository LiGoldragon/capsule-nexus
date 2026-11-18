//! The lane framing every Nexus's translator speaks is `nexus_exchange::lane`;
//! these are the paths the Capsule Nexus's external regressions read it by.

pub use nexus_exchange::lane::HEADER_BYTES;

pub fn peer_caller(
    stream: &std::os::unix::net::UnixStream,
) -> Result<shared_translator::Caller, nexus_exchange::lane::TransportError> {
    use nexus_exchange::lane::PeerCalling;
    stream.peer_caller()
}
