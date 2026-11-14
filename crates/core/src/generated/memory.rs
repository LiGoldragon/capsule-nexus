#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct OwnedPane {
    pub pane: signal_capsule_nexus::Pane,
    pub workspace: signal_capsule_nexus::Workspace,
    pub label_option: std::option::Option<signal_capsule_nexus::Label>,
    pub state: signal_capsule_nexus::State,
    pub attempt: signal_capsule_nexus::Attempt,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Answer {
    Succeeded,
    Failed(signal_capsule_nexus::MemoryRefusal),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct PendingCreation {
    pub creation_id: signal_capsule_nexus::CreationId,
    pub principal: translator::Principal,
    pub workspace: signal_capsule_nexus::Workspace,
}
