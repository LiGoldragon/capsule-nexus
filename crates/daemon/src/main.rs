use nexus_serving::{BootstrapReading, ConnectionServing, MetaServing, NexusBootstrap};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let place = {
        let mut input = std::io::stdin().lock();
        let bootstrap = NexusBootstrap::read_bootstrap(&mut input)?;
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
    // Both sockets' connections, the public socket's subscriptions and the
    // actor that owns the Capsule's state run on this runtime; the accepting
    // stays on threads of its own.
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    let dispatcher = capsule_nexus_daemon::Dispatcher::new(
        bootstrap.paths(),
        bootstrap.configuration_store(),
        signal_capsule_nexus::InstanceId(place.instance_id.0),
        runtime.handle().clone(),
    );

    let meta = Arc::clone(&bootstrap);
    let meta_runtime = runtime.handle().clone();
    std::thread::spawn(move || loop {
        match meta.accept_meta() {
            Ok(stream) => {
                meta_runtime.spawn(capsule_nexus_daemon::MetaConnection::serve(
                    stream,
                    Arc::clone(&meta),
                    meta.admission(),
                ));
            }
            Err(error) => {
                eprintln!("Capsule Nexus meta listener failed permanently: {error}");
                std::process::exit(1);
            }
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
        runtime.spawn(capsule_nexus_daemon::PublicConnection::serve(
            accepted.stream,
            accepted.authority,
            dispatcher.clone(),
            configuration,
        ));
    }
}
