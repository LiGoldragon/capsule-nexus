#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
use shared_translator::placement::Place;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct CoreBootstrap {
    pub place: Place,
    pub admission: nexus_exchange::Admission,
    pub peer_identity: shared_translator::PeerIdentity,
    pub instance_id: signal_capsule_nexus::InstanceId,
}
