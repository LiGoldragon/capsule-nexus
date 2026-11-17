#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum CoreSet {
    StartTimeout(meta_signal_capsule_nexus::Timeout),
    Polling(meta_signal_capsule_nexus::Timeout),
    FrameCapacity(meta_signal_capsule_nexus::ByteLimit),
    TransportTimeout(meta_signal_capsule_nexus::Timeout),
    SubscriberCapacity(i64),
    Peer(translator::PeerBinding),
    RemovePeer(translator::PeerId),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Input {
    Configure(CoreSet),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Configured_Data {
    pub core_set: CoreSet,
    pub readiness: meta_signal_capsule_nexus::Readiness,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Output {
    Configured(Configured_Data),
    ConfigureRefused(meta_signal_capsule_nexus::ConfigurationRefusal),
}
#[rustfmt::skip]
#[allow(clippy::result_large_err, clippy::ptr_arg, clippy::borrowed_box)]
pub trait Answering {
    fn configure(
        &self,
        core_set: &CoreSet,
    ) -> std::result::Result<Configured_Data, Output>;
}
#[rustfmt::skip]
pub trait Answerable {
    type Answer;
    fn answer<Nexus: Answering>(&self, nexus: &Nexus) -> Self::Answer;
}
#[rustfmt::skip]
impl Answerable for Input {
    type Answer = Output;
    fn answer<Nexus: Answering>(&self, nexus: &Nexus) -> Output {
        match self {
            Input::Configure(carried) => {
                nexus
                    .configure(carried)
                    .map_or_else(|refusal| refusal, Output::Configured)
            }
        }
    }
}
/// The generated internal meta contract source.
pub const ETHOS: &str = include_str!("../ethos/core-meta.ethos");

impl signal::Contracted for Input {
    const CONTRACT_SOURCE: &'static str = ETHOS;
}
