use capsule_nexus_translator::CapsuleTranslator;
use translator_serving::{BootstrapReading, TranslatorBootstrap, TranslatorServing};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap = TranslatorBootstrap::read_standard_input()?;
    CapsuleTranslator::default().serve(&bootstrap)?;
    Ok(())
}
