#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Starting_Data {
    pub pane: signal_capsule_nexus::Pane,
    pub command: signal_capsule_nexus::Command,
    pub label: signal_capsule_nexus::Label,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Prompting_Data {
    pub pane: signal_capsule_nexus::Pane,
    pub text: signal_capsule_nexus::Text,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Pressing_Data {
    pub pane: signal_capsule_nexus::Pane,
    pub keys: signal_capsule_nexus::Keys,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Operation {
    Creating(signal_capsule_nexus::Workspace),
    Starting(Starting_Data),
    Prompting(Prompting_Data),
    Pressing(Pressing_Data),
    Closing(signal_capsule_nexus::Pane),
    Listing(signal_capsule_nexus::Session),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Outcome {
    Created(signal_capsule_nexus::Outcome),
    Started(signal_capsule_nexus::Outcome),
    Prompted(signal_capsule_nexus::Outcome),
    Pressed(signal_capsule_nexus::Outcome),
    Closed(signal_capsule_nexus::Outcome),
    Listed(signal_capsule_nexus::Outcome),
}
