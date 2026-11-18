use capsule_nexus_wire::CapsuleNexus;
use translator_client::{AnswerPrinting, Invoking, TextInvocation, TextLane};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    TextInvocation::from_arguments::<CapsuleNexus>(TextLane::MetaText, std::env::args().skip(1))?
        .print_answers()?;
    Ok(())
}
