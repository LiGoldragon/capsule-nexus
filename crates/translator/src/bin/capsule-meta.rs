use capsule_nexus_wire::CapsuleNexus;
use translator_client::{TextCommand, TextInvocation, TextLane};

fn main() -> std::process::ExitCode {
    TextInvocation::run::<CapsuleNexus>(TextLane::MetaText, std::env::args().skip(1))
}
