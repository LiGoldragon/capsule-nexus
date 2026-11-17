use signal::{FrameCapacity, FrameReading, Restorable, Signal};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap_capacity = FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES);
    let place = {
        let mut input = std::io::stdin().lock();
        let body = input.read_frame(bootstrap_capacity)?;
        let bootstrap =
            Signal::<capsule_nexus_wire::transport_types::CoreBootstrap>::from(Vec::<u8>::from(body))
                .restore()?;
        drop(input);
        nix::unistd::close(0)?;
        bootstrap
    };
    let admission = capsule_nexus_wire::ValidAdmission::try_from(&place.admission)
        .map_err(|message| format!("invalid core bootstrap admission: {message}"))?;
    shared_translator::placement::validate_place_name(&place.place.place_name.0)?;
    shared_translator::placement::Preparable::prepare_for_start(&place.place)
        .map_err(|error| format!("place preparation failed: {error}"))?;
    let bootstrap = Arc::new(capsule_nexus_daemon::Bootstrap::bind(
        &place.place,
        admission,
        place.peer_identity,
    )?);
    let dispatcher = Arc::new(capsule_nexus_daemon::Dispatcher::new(
        bootstrap.paths(),
        bootstrap.configuration_store(),
        place.instance_id,
    ));

    let meta = Arc::clone(&bootstrap);
    std::thread::spawn(move || loop {
        if let Err(error) = meta.serve_next_meta() {
            eprintln!("Capsule Nexus meta listener failed permanently: {error}");
            std::process::exit(1);
        }
    });

    loop {
        let accepted = match bootstrap.accept_public() {
            Ok(accepted) => accepted,
            Err(error) => {
                return Err(format!("Capsule Nexus public listener failed permanently: {error}").into());
            }
        };
        let Some(configuration) = bootstrap.snapshot() else {
            continue;
        };
        let dispatcher = Arc::clone(&dispatcher);
        std::thread::spawn(move || {
            match capsule_nexus_daemon::PublicConnection::new(accepted.stream, accepted.authority) {
                Ok(connection) => connection.serve(&dispatcher, &configuration),
                Err(error) => eprintln!("Capsule Nexus public connection failed: {error}"),
            }
        });
    }
}
