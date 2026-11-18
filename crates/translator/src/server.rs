//! The Capsule's translator: its own settings, its bodies for the serving
//! every translator shares, and the herdr calls it makes for the Nexus.

use crate::generated::argument::{Arguable, ListArgument, PaneCloseArgument};
use crate::runner::{self, Captured};
use capsule_nexus_wire::{meta_call, meta_payload, public_call, public_payload, CapsuleNexus};
use core_meta_capsule_nexus::CoreSet;
use emission_capsule_nexus::{
    AgentListing, AgentPrompt_Data, AgentStart_Data, ExternalOutcome, Herdr, PaneListing,
    SendKeys_Data, TabId, TerminalId, Verb, WorkspaceCreate_Data, WorkspaceId, WorkspaceLabel,
    WorkspaceListing,
};
use meta_signal_capsule_nexus::Set;
use serde::Deserialize;
use shared_translator::Caller;
use signal_capsule_nexus::{
    AgentKind, ErrorCode, Executable, ExitStatus, Message, Pane, PaneId, Refusal, Session, State,
};
use std::collections::HashSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use translator_serving::{
    DepthLimit, EmissionAnswering, MetaTranslating, NexusFailure, NodeLimit, PublicTranslating,
    Settling, TextBudgeting, TextLimits, Translator, TranslatorBootstrap, ValidTextBudget,
};

/// The Capsule Nexus's translator, holding its own settings.
pub struct CapsuleTranslator {
    pub configuration: TranslatorConfiguration,
}

/// The translator a bootstrap starts: its text budget starts at the
/// bootstrap's text limits, which the pair launcher takes from its
/// configuration; its other settings await the meta socket.
impl TryFrom<&TranslatorBootstrap> for CapsuleTranslator {
    type Error = &'static str;

    fn try_from(bootstrap: &TranslatorBootstrap) -> Result<Self, Self::Error> {
        ValidTextBudget::try_from(&bootstrap.text_limits).map(Self::from)
    }
}

impl From<ValidTextBudget> for CapsuleTranslator {
    fn from(text_budget: ValidTextBudget) -> Self {
        Self {
            configuration: TranslatorConfiguration::from(text_budget),
        }
    }
}

impl Translator for CapsuleTranslator {
    type Component = CapsuleNexus;
    type Ready = ReadyTranslatorConfiguration;

    fn ready(&self) -> Option<ReadyTranslatorConfiguration> {
        self.configuration.ready()
    }
}

impl PublicTranslating for CapsuleTranslator {
    type PublicInput = signal_capsule_nexus::Input;
    type PublicOutput = signal_capsule_nexus::Output;
    type PublicCall = public_call::Input;
    type PublicResponse = public_call::Output;

    fn public_call(caller: Caller, input: signal_capsule_nexus::Input) -> public_call::Input {
        public_call::Input::Call(public_call::Call_Data {
            caller,
            public_input: public_payload::Input(input),
        })
    }

    fn public_answered(
        response: public_call::Output,
    ) -> Result<signal_capsule_nexus::Output, NexusFailure> {
        match response {
            public_call::Output::Answered(output) => Ok(output.0),
            public_call::Output::Failed(message) => Err(NexusFailure(message)),
        }
    }
}

impl MetaTranslating for CapsuleTranslator {
    type MetaInput = meta_signal_capsule_nexus::Input;
    type MetaOutput = meta_signal_capsule_nexus::Output;
    type NexusMetaInput = CoreSet;
    type MetaCall = meta_call::Input;
    type MetaResponse = meta_call::Output;

    /// The herdr path, the call timeout, the output limit and the text budget
    /// are the translator's own; every other setting is the Nexus's.
    fn settle(
        &self,
        input: meta_signal_capsule_nexus::Input,
    ) -> Settling<meta_signal_capsule_nexus::Output, CoreSet> {
        let meta_signal_capsule_nexus::Input::Configure(setting) = input;
        match setting {
            setting @ (Set::HerdrPath(_)
            | Set::CallTimeout(_)
            | Set::OutputLimit(_)
            | Set::TextBudget(_)) => Settling::Answered(self.configuration.apply(setting)),
            Set::StartTimeout(value) => Settling::Forwarded(CoreSet::StartTimeout(value)),
            Set::Polling(value) => Settling::Forwarded(CoreSet::Polling(value)),
            Set::FrameCapacity(value) => Settling::Forwarded(CoreSet::FrameCapacity(value)),
            Set::TransportTimeout(value) => Settling::Forwarded(CoreSet::TransportTimeout(value)),
            Set::SubscriberCapacity(value) => {
                Settling::Forwarded(CoreSet::SubscriberCapacity(value))
            }
            Set::Peer(value) => Settling::Forwarded(CoreSet::Peer(value)),
            Set::RemovePeer(value) => Settling::Forwarded(CoreSet::RemovePeer(value)),
        }
    }

    fn meta_call(caller: Caller, input: CoreSet) -> meta_call::Input {
        meta_call::Input::Call(meta_call::Call_Data {
            caller,
            meta_input: meta_payload::Input(core_meta_capsule_nexus::Input::Configure(input)),
        })
    }

    /// The Nexus's configured setting, named as the meta contract names it
    /// and owned by the Nexus, its readiness the Capsule's whole: the
    /// translator's own settings still awaited, then the Nexus's; its
    /// refusal as it stands.
    fn meta_answered(
        &self,
        response: meta_call::Output,
    ) -> Result<meta_signal_capsule_nexus::Output, NexusFailure> {
        match response {
            meta_call::Output::Answered(output) => Ok(match output.0 {
                core_meta_capsule_nexus::Output::Configured(configured) => {
                    meta_signal_capsule_nexus::Output::Configured(
                        meta_signal_capsule_nexus::Configured_Data {
                            set: public_setting(configured.core_set),
                            owner: meta_signal_capsule_nexus::Owner::Nexus,
                            readiness: self.configuration.merged(configured.readiness),
                        },
                    )
                }
                core_meta_capsule_nexus::Output::ConfigureRefused(refusal) => {
                    meta_signal_capsule_nexus::Output::ConfigureRefused(refusal)
                }
            }),
            meta_call::Output::Failed(message) => Err(NexusFailure(message)),
        }
    }

    /// The meta contract has no subscription to carry a change into.
    fn announced(
        &self,
        _answered: &meta_signal_capsule_nexus::Output,
    ) -> Option<meta_signal_capsule_nexus::Output> {
        None
    }
}

impl EmissionAnswering for CapsuleTranslator {
    type Emission = emission_capsule_nexus::Input;
    type Outcome = emission_capsule_nexus::Output;

    fn emitted(
        &self,
        ready: &ReadyTranslatorConfiguration,
        emission: emission_capsule_nexus::Input,
    ) -> emission_capsule_nexus::Output {
        let emission_capsule_nexus::Input::Emit(herdr) = emission;
        emission_capsule_nexus::Output::Emitted(execute(&ready.runtime(), &herdr))
    }
}

fn public_setting(setting: CoreSet) -> Set {
    match setting {
        CoreSet::StartTimeout(value) => Set::StartTimeout(value),
        CoreSet::Polling(value) => Set::Polling(value),
        CoreSet::FrameCapacity(value) => Set::FrameCapacity(value),
        CoreSet::TransportTimeout(value) => Set::TransportTimeout(value),
        CoreSet::SubscriberCapacity(value) => Set::SubscriberCapacity(value),
        CoreSet::Peer(value) => Set::Peer(value),
        CoreSet::RemovePeer(value) => Set::RemovePeer(value),
    }
}

/// The translator's own settings: the herdr path, the call timeout and the
/// output limit, set on the meta socket, and the text budget, which starts
/// at the bootstrap's and is set on the meta socket after.
pub struct TranslatorConfiguration {
    partial: std::sync::RwLock<PartialTranslatorConfiguration>,
}

#[derive(Clone)]
struct PartialTranslatorConfiguration {
    executable: Option<PathBuf>,
    call_timeout: Option<Duration>,
    output_limit: Option<usize>,
    text_budget: ValidTextBudget,
}

impl From<ValidTextBudget> for TranslatorConfiguration {
    fn from(text_budget: ValidTextBudget) -> Self {
        Self {
            partial: std::sync::RwLock::new(PartialTranslatorConfiguration {
                executable: None,
                call_timeout: None,
                output_limit: None,
                text_budget,
            }),
        }
    }
}

/// The translator's settings once every one is set.
#[derive(Clone)]
pub struct ReadyTranslatorConfiguration {
    executable: PathBuf,
    call_timeout: Duration,
    output_limit: usize,
    text_budget: ValidTextBudget,
}

impl ReadyTranslatorConfiguration {
    fn runtime(&self) -> Runtime {
        Runtime::new(self.executable.clone(), self.call_timeout, self.output_limit)
            .expect("ready translator configuration was validated before publication")
    }
}

impl TextBudgeting for ReadyTranslatorConfiguration {
    fn text_budget(&self) -> ValidTextBudget {
        self.text_budget
    }
}

impl TranslatorConfiguration {
    pub fn ready(&self) -> Option<ReadyTranslatorConfiguration> {
        let partial = self.partial.read().ok()?.clone();
        let ready = ReadyTranslatorConfiguration {
            executable: partial.executable?,
            call_timeout: partial.call_timeout?,
            output_limit: partial.output_limit?,
            text_budget: partial.text_budget,
        };
        Runtime::new(ready.executable.clone(), ready.call_timeout, ready.output_limit)?;
        Some(ready)
    }

    pub fn apply(&self, setting: Set) -> meta_signal_capsule_nexus::Output {
        use meta_signal_capsule_nexus::{Configured_Data, Output, Owner, SettingName};
        let applied = setting.clone();
        let mut partial = self
            .partial
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let invalid = match setting {
            Set::HerdrPath(value) if Path::new(&value.0).is_absolute() => {
                partial.executable = Some(PathBuf::from(value.0));
                None
            }
            Set::CallTimeout(value) => positive_duration(value.0)
                .map(|value| {
                    partial.call_timeout = Some(value);
                })
                .is_none()
                .then_some(SettingName::CallTimeout),
            Set::OutputLimit(value) => usize::try_from(value.0)
                .ok()
                .filter(|value| *value > 0)
                .map(|value| {
                    partial.output_limit = Some(value);
                })
                .is_none()
                .then_some(SettingName::OutputLimit),
            Set::TextBudget(value) => match ValidTextBudget::try_from(&TextLimits {
                node_limit: NodeLimit(value.node_limit.0),
                depth_limit: DepthLimit(value.depth_limit.0),
            }) {
                Ok(budget) => {
                    partial.text_budget = budget;
                    None
                }
                Err(_) => Some(SettingName::TextBudget),
            },
            Set::HerdrPath(_) => Some(SettingName::HerdrPath),
            other => {
                return Output::ConfigureRefused(invalid_setting(
                    Owner::Translator,
                    setting_name(&other),
                    "setting belongs to Nexus",
                ))
            }
        };
        if let Some(name) = invalid {
            return Output::ConfigureRefused(invalid_setting(
                Owner::Translator,
                name,
                "invalid setting value",
            ));
        }
        let awaiting = partial.awaiting();
        drop(partial);
        Output::Configured(Configured_Data {
            set: applied,
            owner: Owner::Translator,
            readiness: readiness(awaiting),
        })
    }

    /// The readiness the Nexus answered, with the translator's own settings
    /// still awaited first among the awaited: the Capsule is ready when both
    /// are.
    pub fn merged(
        &self,
        nexus: meta_signal_capsule_nexus::Readiness,
    ) -> meta_signal_capsule_nexus::Readiness {
        let mut awaiting = self
            .partial
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .awaiting();
        if let meta_signal_capsule_nexus::Readiness::Awaiting(names) = nexus {
            awaiting.extend(names);
        }
        readiness(awaiting)
    }
}

impl PartialTranslatorConfiguration {
    /// The translator's own settings not yet set.
    fn awaiting(&self) -> Vec<meta_signal_capsule_nexus::SettingName> {
        use meta_signal_capsule_nexus::SettingName;
        [
            (self.executable.is_none(), SettingName::HerdrPath),
            (self.call_timeout.is_none(), SettingName::CallTimeout),
            (self.output_limit.is_none(), SettingName::OutputLimit),
        ]
        .into_iter()
        .filter_map(|(unset, name)| unset.then_some(name))
        .collect()
    }
}

/// Ready when nothing is awaited.
fn readiness(
    awaiting: Vec<meta_signal_capsule_nexus::SettingName>,
) -> meta_signal_capsule_nexus::Readiness {
    if awaiting.is_empty() {
        meta_signal_capsule_nexus::Readiness::Ready
    } else {
        meta_signal_capsule_nexus::Readiness::Awaiting(awaiting)
    }
}

fn setting_name(setting: &Set) -> meta_signal_capsule_nexus::SettingName {
    use meta_signal_capsule_nexus::SettingName;
    match setting {
        Set::HerdrPath(_) => SettingName::HerdrPath,
        Set::CallTimeout(_) => SettingName::CallTimeout,
        Set::OutputLimit(_) => SettingName::OutputLimit,
        Set::TextBudget(_) => SettingName::TextBudget,
        Set::StartTimeout(_) => SettingName::StartTimeout,
        Set::Polling(_) => SettingName::Polling,
        Set::FrameCapacity(_) => SettingName::FrameCapacity,
        Set::TransportTimeout(_) => SettingName::TransportTimeout,
        Set::SubscriberCapacity(_) => SettingName::SubscriberCapacity,
        Set::Peer(_) | Set::RemovePeer(_) => SettingName::Peer,
    }
}

fn invalid_setting(
    owner: meta_signal_capsule_nexus::Owner,
    setting_name: meta_signal_capsule_nexus::SettingName,
    message: &str,
) -> meta_signal_capsule_nexus::ConfigurationRefusal {
    meta_signal_capsule_nexus::ConfigurationRefusal::Invalid(meta_signal_capsule_nexus::Invalid_Data {
        owner,
        setting_name,
        message: meta_signal_capsule_nexus::Message(message.into()),
    })
}

/// Executes one typed external emission with a captured configuration snapshot.
pub fn execute(runtime: &Runtime, herdr: &Herdr) -> ExternalOutcome {
    match &herdr.verb {
        Verb::WorkspaceCreate(command) => execute_command(runtime, &herdr.session, command),
        Verb::WorkspaceList => execute_command(runtime, &herdr.session, &ListArgument::Workspace),
        Verb::AgentStart(command) => execute_command(runtime, &herdr.session, command),
        Verb::AgentPrompt(command) => execute_command(runtime, &herdr.session, command),
        Verb::SendKeys(command) => execute_command(runtime, &herdr.session, command),
        Verb::PaneClose(pane_id) => execute_command(
            runtime,
            &herdr.session,
            &PaneCloseArgument(pane_id.clone()),
        ),
        Verb::PaneList => execute_command(runtime, &herdr.session, &ListArgument::Pane),
        Verb::AgentList => execute_command(runtime, &herdr.session, &ListArgument::Agent),
    }
}

#[derive(Clone)]
pub struct Runtime {
    pub executable: PathBuf,
    pub call_timeout: Duration,
    pub output_limit: usize,
}

impl Runtime {
    pub fn new(executable: PathBuf, call_timeout: Duration, output_limit: usize) -> Option<Self> {
        if executable.as_os_str().is_empty() || call_timeout.is_zero() || output_limit == 0 {
            return None;
        }
        Some(Self {
            executable,
            call_timeout,
            output_limit,
        })
    }
}

fn positive_duration(milliseconds: i64) -> Option<Duration> {
    let milliseconds = u64::try_from(milliseconds).ok()?;
    let duration = Duration::from_millis(milliseconds);
    (!duration.is_zero() && Instant::now().checked_add(duration).is_some()).then_some(duration)
}

impl Arguable for WorkspaceCreate_Data {
    fn arguments(&self) -> Vec<String> {
        let mut arguments = vec![
            "workspace".into(),
            "create".into(),
            "--cwd".into(),
            self.root.0.clone(),
            "--label".into(),
            emission_capsule_nexus::managed_workspace_label(&self.creation_id).0,
        ];
        for variable in &self.variable_vector {
            arguments.push("--env".into());
            arguments.push(format!("{}={}", variable.name.0, variable.value.0));
        }
        arguments
    }
}

impl Arguable for AgentStart_Data {
    fn arguments(&self) -> Vec<String> {
        let mut arguments = vec![
            "agent".into(),
            "start".into(),
            self.command.agent_name.0.clone(),
            "--kind".into(),
            agent_kind(&self.command.agent_kind).into(),
            "--pane".into(),
            self.pane_id.0.clone(),
        ];
        if let Executable::Explicit(program) = &self.command.executable {
            arguments.push("--executable".into());
            arguments.push(program.0.clone());
        }
        arguments.push("--timeout".into());
        arguments.push(self.timeout.0.to_string());
        if !self.command.string_vector.is_empty() {
            arguments.push("--".into());
            arguments.extend(self.command.string_vector.iter().cloned());
        }
        arguments
    }
}

impl Arguable for AgentPrompt_Data {
    fn arguments(&self) -> Vec<String> {
        vec![
            "agent".into(),
            "prompt".into(),
            self.pane_id.0.clone(),
            self.text.0.clone(),
        ]
    }
}

impl Arguable for SendKeys_Data {
    fn arguments(&self) -> Vec<String> {
        let mut arguments = vec![
            "pane".into(),
            "send-keys".into(),
            self.pane_id.0.clone(),
        ];
        arguments.extend(self.keys.0.iter().cloned());
        arguments
    }
}

impl Arguable for PaneCloseArgument {
    fn arguments(&self) -> Vec<String> {
        vec![
            "pane".into(),
            "close".into(),
            self.0.0.clone(),
        ]
    }
}

impl Arguable for ListArgument {
    fn arguments(&self) -> Vec<String> {
        let collection = match self {
            Self::Workspace => "workspace",
            Self::Pane => "pane",
            Self::Agent => "agent",
        };
        vec![collection.into(), "list".into()]
    }
}

trait ExternalCommand: Arguable {
    fn timeout(&self, runtime: &Runtime) -> Option<Duration> {
        Some(runtime.call_timeout)
    }
    fn response_id(&self) -> &'static str;
    fn decode_success(&self, stdout: &[u8], session: &Session) -> ExternalOutcome;
}

fn execute_command<T: ExternalCommand>(runtime: &Runtime, session: &Session, command: &T) -> ExternalOutcome {
    let Some(timeout) = command.timeout(runtime) else {
        return ExternalOutcome::Uncertain;
    };
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return ExternalOutcome::Uncertain;
    };
    let mut arguments = vec![OsString::from("--session"), OsString::from(&session.0)];
    arguments.extend(command.arguments().into_iter().map(OsString::from));
    let captured = match runner::run(&runtime.executable, &arguments, deadline, runtime.output_limit) {
        Ok(captured) => captured,
        Err(error) => {
            eprintln!("herdr client execution failed: {error}");
            return ExternalOutcome::Uncertain;
        }
    };
    if !captured.status.success() {
        return refusal_or_uncertain(&captured, command.response_id());
    }
    command.decode_success(&captured.stdout, session)
}

impl ExternalCommand for WorkspaceCreate_Data {
    fn response_id(&self) -> &'static str { "cli:workspace:create" }
    fn decode_success(&self, stdout: &[u8], session: &Session) -> ExternalOutcome {
        decode_workspace(stdout, session)
    }
}

impl ExternalCommand for AgentStart_Data {
    fn timeout(&self, _runtime: &Runtime) -> Option<Duration> { positive_duration(self.timeout.0) }
    fn response_id(&self) -> &'static str { "cli:agent:start" }
    fn decode_success(&self, stdout: &[u8], _session: &Session) -> ExternalOutcome {
        decode_start(
            stdout,
            &self.pane_id.0,
            &self.command.agent_name.0,
            agent_kind(&self.command.agent_kind),
        )
    }
}

impl ExternalCommand for AgentPrompt_Data {
    fn response_id(&self) -> &'static str { "cli:agent:prompt" }
    fn decode_success(&self, stdout: &[u8], _session: &Session) -> ExternalOutcome {
        decode_prompt(stdout, &self.pane_id.0)
    }
}

impl ExternalCommand for SendKeys_Data {
    fn response_id(&self) -> &'static str { "cli:pane:send-keys" }
    fn decode_success(&self, stdout: &[u8], _session: &Session) -> ExternalOutcome {
        if stdout.is_empty() { ExternalOutcome::Done } else { ExternalOutcome::Uncertain }
    }
}

impl ExternalCommand for PaneCloseArgument {
    fn response_id(&self) -> &'static str { "cli:pane:close" }
    fn decode_success(&self, stdout: &[u8], _session: &Session) -> ExternalOutcome {
        decode_ok(stdout, Some(self.response_id()))
    }
}

impl ExternalCommand for ListArgument {
    fn response_id(&self) -> &'static str {
        match self {
            Self::Workspace => "cli:workspace:list",
            Self::Pane => "cli:pane:list",
            Self::Agent => "cli:agent:list",
        }
    }
    fn decode_success(&self, stdout: &[u8], session: &Session) -> ExternalOutcome {
        match self {
            Self::Workspace => decode_workspaces(stdout),
            Self::Pane => decode_panes(stdout, session),
            Self::Agent => decode_agents(stdout, session),
        }
    }
}

fn refusal_or_uncertain(captured: &Captured, expected_id: &str) -> ExternalOutcome {
    // Herdr's failure contract is one JSON envelope on stderr. Stdout is the
    // success channel and is never tried as a fallback failure language.
    let envelope = serde_json::from_slice::<ErrorEnvelope>(&captured.stderr);
    let Ok(envelope) = envelope else {
        eprintln!(
            "herdr refusal was not its stderr JSON contract (status={:?}, stdout_bytes={}, stderr_bytes={})",
            captured.status.code(),
            captured.stdout.len(),
            captured.stderr.len(),
        );
        return ExternalOutcome::Uncertain;
    };
    if envelope.id != expected_id {
        return ExternalOutcome::Uncertain;
    }
    ExternalOutcome::Refused(Refusal {
        error_code: ErrorCode(envelope.error.code),
        exit_status_option: captured
            .status
            .code()
            .map(|code| ExitStatus(i64::from(code))),
        message: Message(envelope.error.message),
    })
}

#[derive(Deserialize)]
struct Envelope<T> {
    id: String,
    result: T,
}

#[derive(Deserialize)]
struct ErrorEnvelope {
    #[allow(dead_code)]
    id: String,
    error: ErrorBody,
}

#[derive(Deserialize)]
struct ErrorBody {
    code: String,
    message: String,
}

#[derive(Clone, Deserialize)]
struct PaneJson {
    pane_id: String,
    terminal_id: String,
    workspace_id: String,
    tab_id: String,
    #[allow(dead_code)]
    focused: bool,
    #[allow(dead_code)]
    agent_status: AgentStatus,
    #[allow(dead_code)]
    revision: u64,
}

#[derive(Clone, Deserialize)]
struct AgentJson {
    terminal_id: String,
    name: Option<String>,
    agent: Option<String>,
    agent_status: AgentStatus,
    workspace_id: String,
    tab_id: String,
    pane_id: String,
    #[allow(dead_code)]
    focused: bool,
    #[serde(default)]
    interactive_ready: bool,
    #[allow(dead_code)]
    revision: u64,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum AgentStatus {
    Idle,
    Working,
    Blocked,
    Done,
    Unknown,
}

#[derive(Deserialize)]
struct WorkspaceResult {
    #[serde(rename = "type")]
    kind: String,
    #[allow(dead_code)]
    workspace: serde_json::Value,
    #[allow(dead_code)]
    tab: serde_json::Value,
    root_pane: PaneJson,
}

#[derive(Deserialize)]
struct WorkspaceListResult {
    #[serde(rename = "type")]
    kind: String,
    workspaces: Vec<WorkspaceListItem>,
}

#[derive(Deserialize)]
struct WorkspaceListItem {
    workspace_id: String,
    label: String,
}

#[derive(Deserialize)]
struct StartResult {
    #[serde(rename = "type")]
    kind: String,
    agent: AgentJson,
    #[allow(dead_code)]
    argv: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)]
    executable: Option<String>,
}

#[derive(Deserialize)]
struct PromptResult {
    #[serde(rename = "type")]
    kind: String,
    agent: AgentJson,
}

#[derive(Deserialize)]
struct OkResult {
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Deserialize)]
struct PaneListResult {
    #[serde(rename = "type")]
    kind: String,
    panes: Vec<PaneJson>,
}

#[derive(Deserialize)]
struct AgentListResult {
    #[serde(rename = "type")]
    kind: String,
    agents: Vec<AgentJson>,
}

fn decode_workspace(bytes: &[u8], session: &Session) -> ExternalOutcome {
    let Ok(envelope) = serde_json::from_slice::<Envelope<WorkspaceResult>>(bytes) else {
        return ExternalOutcome::Uncertain;
    };
    if envelope.id != "cli:workspace:create" || envelope.result.kind != "workspace_created" {
        return ExternalOutcome::Uncertain;
    }
    ExternalOutcome::Created(Pane {
        session: session.clone(),
        pane_id: PaneId(envelope.result.root_pane.pane_id),
    })
}

fn decode_workspaces(bytes: &[u8]) -> ExternalOutcome {
    let Ok(envelope) = serde_json::from_slice::<Envelope<WorkspaceListResult>>(bytes) else {
        return ExternalOutcome::Uncertain;
    };
    if envelope.id != "cli:workspace:list" || envelope.result.kind != "workspace_list" {
        return ExternalOutcome::Uncertain;
    }
    let mut workspace_ids = HashSet::new();
    let mut listings = Vec::with_capacity(envelope.result.workspaces.len());
    for workspace in envelope.result.workspaces {
        if !workspace_ids.insert(workspace.workspace_id.clone()) {
            return ExternalOutcome::Uncertain;
        }
        listings.push(WorkspaceListing {
            workspace_id: WorkspaceId(workspace.workspace_id),
            workspace_label: WorkspaceLabel(workspace.label),
        });
    }
    ExternalOutcome::Workspaces(listings)
}

fn decode_start(bytes: &[u8], pane_id: &str, name: &str, kind: &str) -> ExternalOutcome {
    let Ok(envelope) = serde_json::from_slice::<Envelope<StartResult>>(bytes) else {
        return ExternalOutcome::Uncertain;
    };
    let result = envelope.result;
    if envelope.id != "cli:agent:start"
        || result.kind != "agent_started"
        || result.agent.pane_id != pane_id
        || result.agent.name.as_deref() != Some(name)
        || result.agent.agent.as_deref() != Some(kind)
        || !result.agent.interactive_ready
    {
        return ExternalOutcome::Uncertain;
    }
    ExternalOutcome::Started(agent_state(&result.agent))
}

fn decode_prompt(bytes: &[u8], pane_id: &str) -> ExternalOutcome {
    let Ok(envelope) = serde_json::from_slice::<Envelope<PromptResult>>(bytes) else {
        return ExternalOutcome::Uncertain;
    };
    if envelope.id == "cli:agent:prompt"
        && envelope.result.kind == "agent_prompted"
        && envelope.result.agent.pane_id == pane_id
    {
        ExternalOutcome::Done
    } else {
        ExternalOutcome::Uncertain
    }
}

fn decode_ok(bytes: &[u8], expected_id: Option<&str>) -> ExternalOutcome {
    let Ok(envelope) = serde_json::from_slice::<Envelope<OkResult>>(bytes) else {
        return ExternalOutcome::Uncertain;
    };
    if envelope.result.kind == "ok" && expected_id.is_none_or(|expected| envelope.id == expected) {
        ExternalOutcome::Done
    } else {
        ExternalOutcome::Uncertain
    }
}

fn decode_panes(bytes: &[u8], session: &Session) -> ExternalOutcome {
    let Ok(envelope) = serde_json::from_slice::<Envelope<PaneListResult>>(bytes) else {
        return ExternalOutcome::Uncertain;
    };
    if envelope.id != "cli:pane:list" || envelope.result.kind != "pane_list" {
        return ExternalOutcome::Uncertain;
    }
    let mut pane_ids = HashSet::new();
    for pane in &envelope.result.panes {
        if !pane_ids.insert(pane.pane_id.as_str()) {
            return ExternalOutcome::Uncertain;
        }
    }
    ExternalOutcome::Panes(
        envelope
            .result
            .panes
            .into_iter()
            .map(|pane| PaneListing {
                pane: Pane {
                    session: session.clone(),
                    pane_id: PaneId(pane.pane_id),
                },
                terminal_id: TerminalId(pane.terminal_id),
                workspace_id: WorkspaceId(pane.workspace_id),
                tab_id: TabId(pane.tab_id),
            })
            .collect(),
    )
}

fn decode_agents(bytes: &[u8], session: &Session) -> ExternalOutcome {
    let Ok(envelope) = serde_json::from_slice::<Envelope<AgentListResult>>(bytes) else {
        return ExternalOutcome::Uncertain;
    };
    if envelope.id != "cli:agent:list" || envelope.result.kind != "agent_list" {
        return ExternalOutcome::Uncertain;
    }
    let mut pane_ids = HashSet::new();
    let mut listings = Vec::with_capacity(envelope.result.agents.len());
    for agent in envelope.result.agents {
        if !pane_ids.insert(agent.pane_id.clone()) {
            return ExternalOutcome::Uncertain;
        }
        let state = agent_state(&agent);
        listings.push(AgentListing {
            pane: Pane {
                session: session.clone(),
                pane_id: PaneId(agent.pane_id),
            },
            terminal_id: TerminalId(agent.terminal_id),
            workspace_id: WorkspaceId(agent.workspace_id),
            tab_id: TabId(agent.tab_id),
            state,
        });
    }
    ExternalOutcome::Agents(listings)
}

fn agent_state(agent: &AgentJson) -> State {
    match agent.agent_status {
        AgentStatus::Working | AgentStatus::Blocked => State::Busy,
        AgentStatus::Idle | AgentStatus::Done if agent.interactive_ready => State::Idle,
        AgentStatus::Idle | AgentStatus::Done | AgentStatus::Unknown => State::Open,
    }
}

fn agent_kind(kind: &AgentKind) -> &'static str {
    match kind {
        AgentKind::Pi => "pi",
        AgentKind::Claude => "claude",
        AgentKind::Codex => "codex",
        AgentKind::Gemini => "gemini",
        AgentKind::Cursor => "cursor",
        AgentKind::Devin => "devin",
        AgentKind::Agy => "agy",
        AgentKind::Cline => "cline",
        AgentKind::Omp => "omp",
        AgentKind::Mastracode => "mastracode",
        AgentKind::Opencode => "opencode",
        AgentKind::Copilot => "copilot",
        AgentKind::Kimi => "kimi",
        AgentKind::Kiro => "kiro",
        AgentKind::Droid => "droid",
        AgentKind::Amp => "amp",
        AgentKind::Grok => "grok",
        AgentKind::Hermes => "hermes",
        AgentKind::Kilo => "kilo",
        AgentKind::Qodercli => "qodercli",
        AgentKind::Qwen => "qwen",
        AgentKind::Maki => "maki",
    }
}

#[cfg(test)]
mod tests {
    const MAXIMUM_SIGNAL_BYTES: usize = signal::MAXIMUM_SIGNAL_BYTES;
    use super::*;
    use shared_translator::placement::Place;
    use shared_translator::FrameCapacity;
    use std::collections::HashMap;
    use std::os::unix::net::{UnixListener, UnixStream};
    use translator_client::{TextClient, TextLane, TextRequesting};
    use translator_serving::{Connection, ConnectionServing, Endpoint, MetaSubscriptions};

    fn place(runtime: &str) -> Place {
        Place {
            place_name: shared_translator::placement::PlaceName("test".into()),
            runtime: shared_translator::placement::Runtime(runtime.into()),
            state: shared_translator::placement::State(runtime.into()),
        }
    }

    fn admission() -> nexus_exchange::ValidAdmission {
        nexus_exchange::ValidAdmission {
            deadline: Duration::from_secs(1),
            frame_capacity: MAXIMUM_SIGNAL_BYTES,
            retry_delay: Duration::from_millis(5),
        }
    }

    /// Serves one request, unpaired, as the translator socket `endpoint`.
    fn serve(
        stream: &mut UnixStream,
        translator: &CapsuleTranslator,
        place: &Place,
        endpoint: Endpoint,
    ) {
        Connection {
            translator,
            place,
            endpoint,
            admission: admission(),
            bootstrap_budget: ValidTextBudget::MAXIMUM,
            nexus: None,
            subscriptions: &MetaSubscriptions::default(),
        }
        .serve(stream)
        .expect("isolated request served");
    }

    fn meta_request(translator: &CapsuleTranslator, text: &str) -> crate::transport::Frame {
        use crate::transport::{Frame, LaneReading, LaneWriting};
        use crate::transport::{Kind, Lane};
        let (mut client, mut server) = UnixStream::pair().expect("socket pair");
        LaneWriting::write_lane_frame(
            &mut client,
            &Frame {
                lane: Lane::MetaText,
                kind: Kind::Request,
                payload: text.as_bytes().to_vec(),
            },
            MAXIMUM_SIGNAL_BYTES,
        )
        .expect("request frame");
        serve(&mut server, translator, &place("/tmp"), Endpoint::Meta);
        LaneReading::read_lane_frame(&mut client, MAXIMUM_SIGNAL_BYTES).expect("response frame")
    }

    #[test]
    fn meta_lane_is_rejected_on_the_public_endpoint() {
        use crate::transport::{Frame, LaneReading, LaneWriting};
        use crate::transport::{Kind, Lane};
        let (mut client, mut server) = UnixStream::pair().expect("socket pair");
        LaneWriting::write_lane_frame(
            &mut client,
            &Frame {
                lane: Lane::MetaText,
                kind: Kind::Request,
                payload: b"Configure.HerdrPath./bin/true".to_vec(),
            },
            MAXIMUM_SIGNAL_BYTES,
        )
        .expect("request frame");
        serve(
            &mut server,
            &CapsuleTranslator::from(ValidTextBudget::MAXIMUM),
            &place("/tmp"),
            Endpoint::Public,
        );
        assert_eq!(
            LaneReading::read_lane_frame(&mut client, MAXIMUM_SIGNAL_BYTES)
                .expect("response")
                .kind,
            Kind::Failed
        );
    }

    #[test]
    fn malformed_meta_request_is_failed_and_next_valid_set_is_answered() {
        use crate::transport::Kind;
        let translator = CapsuleTranslator::from(ValidTextBudget::MAXIMUM);
        let malformed = meta_request(&translator, "this is not datom");
        assert_eq!(malformed.kind, Kind::Failed);

        let query =
            meta_signal_capsule_nexus::Input::Configure(meta_signal_capsule_nexus::Set::HerdrPath(
                meta_signal_capsule_nexus::PathValue("/usr/bin/herdr".into()),
            ));
        let answered = meta_request(&translator, &shared_translator::encode(&query));
        assert_eq!(answered.kind, Kind::Answered);
        let text = std::str::from_utf8(&answered.payload).expect("UTF-8 response");
        assert!(text.starts_with("Configured."), "{text}");
    }

    #[test]
    fn herdr_path_must_be_absolute_and_failed_set_preserves_prior_value() {
        use meta_signal_capsule_nexus::{PathValue, Output, Set};
        let configuration = TranslatorConfiguration::from(ValidTextBudget::MAXIMUM);
        assert!(matches!(
            configuration.apply(Set::HerdrPath(PathValue("/usr/bin/herdr".into()))),
            Output::Configured(_)
        ));
        assert!(matches!(
            configuration.apply(Set::HerdrPath(PathValue("relative/herdr".into()))),
            Output::ConfigureRefused(_)
        ));
        assert_eq!(
            configuration
                .partial
                .read()
                .expect("configuration lock")
                .executable
                .as_deref(),
            Some(Path::new("/usr/bin/herdr"))
        );
    }

    fn pane(id: &str, status: &str) -> String {
        format!(
            r#"{{"pane_id":"{id}","terminal_id":"t-{id}","workspace_id":"w","tab_id":"tab","focused":false,"agent_status":"{status}","revision":1}}"#
        )
    }

    fn agent(id: &str, status: &str, ready: bool) -> String {
        format!(
            r#"{{"pane_id":"{id}","terminal_id":"t-{id}","workspace_id":"w","tab_id":"tab","focused":false,"agent_status":"{status}","interactive_ready":{ready},"revision":1}}"#
        )
    }

    #[test]
    fn separate_list_decoders_preserve_readiness_and_identity() {
        let panes = format!(
            r#"{{"id":"cli:pane:list","result":{{"type":"pane_list","panes":[{},{},{}]}}}}"#,
            pane("idle", "idle"),
            pane("blocked", "blocked"),
            pane("plain", "unknown")
        );
        let agents = format!(
            r#"{{"id":"cli:agent:list","result":{{"type":"agent_list","agents":[{},{}]}}}}"#,
            agent("idle", "idle", true),
            agent("blocked", "blocked", false)
        );
        let ExternalOutcome::Panes(panes) = decode_panes(panes.as_bytes(), &Session("main".into()))
        else {
            panic!("pane listing expected");
        };
        assert_eq!(panes.len(), 3);
        let ExternalOutcome::Agents(agents) = decode_agents(agents.as_bytes(), &Session("main".into()))
        else {
            panic!("agent listing expected");
        };
        let states: HashMap<_, _> = agents
            .into_iter()
            .map(|item| (item.pane.pane_id.0, item.state))
            .collect();
        assert_eq!(states["idle"], State::Idle);
        assert_eq!(states["blocked"], State::Busy);
    }

    #[test]
    fn duplicate_entries_are_uncertain_within_each_external_result() {
        let one = pane("p", "unknown");
        let duplicate = format!(
            r#"{{"id":"cli:pane:list","result":{{"type":"pane_list","panes":[{one},{one}]}}}}"#
        );
        assert!(matches!(
            decode_panes(duplicate.as_bytes(), &Session("main".into())),
            ExternalOutcome::Uncertain
        ));

        let duplicate_agent = agent("other", "idle", true);
        let agents = format!(
            r#"{{"id":"cli:agent:list","result":{{"type":"agent_list","agents":[{duplicate_agent},{duplicate_agent}]}}}}"#,
        );
        assert!(matches!(
            decode_agents(agents.as_bytes(), &Session("main".into())),
            ExternalOutcome::Uncertain
        ));
    }

    #[test]
    fn unknown_status_is_uncertain() {
        let panes = format!(
            r#"{{"id":"cli:pane:list","result":{{"type":"pane_list","panes":[{}]}}}}"#,
            pane("p", "impossible")
        );
        assert!(matches!(
            decode_panes(panes.as_bytes(), &Session("main".into())),
            ExternalOutcome::Uncertain
        ));
    }

    #[test]
    fn start_accepts_the_minimal_and_convenience_envelopes() {
        let agent = r#"{"terminal_id":"t","name":"probe","agent":"codex","agent_status":"idle","workspace_id":"w","tab_id":"tab","pane_id":"w:p","focused":true,"interactive_ready":true,"revision":2}"#;
        let minimal = format!(
            r#"{{"id":"cli:agent:start","result":{{"type":"agent_started","agent":{agent},"argv":["codex"]}}}}"#
        );
        let convenience = format!(
            r#"{{"id":"cli:agent:start","result":{{"type":"agent_started","agent":{agent},"argv":["codex"],"executable":"codex"}}}}"#
        );
        assert_eq!(
            decode_start(minimal.as_bytes(), "w:p", "probe", "codex"),
            ExternalOutcome::Started(State::Idle)
        );
        assert_eq!(
            decode_start(convenience.as_bytes(), "w:p", "probe", "codex"),
            ExternalOutcome::Started(State::Idle)
        );
    }

    #[test]
    fn actual_text_front_start_outlives_generic_call_timeout() {
        use meta_signal_capsule_nexus::{
            BudgetValue, ByteLimit, DepthLimit, NodeLimit, PathValue, Set, Timeout,
        };
        use shared_translator::{FrameReading, FrameWriting, Restorable, Signal, Signalizable};

        let directory = std::env::temp_dir().join(format!(
            "herdr-front-start-timeout-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir(&directory).expect("fixture directory");
        let core_path = directory.join("capsule.sock");
        let front_path = directory.join(
            capsule_nexus_wire::socket_names()
                .public_translator_socket
                .0,
        );
        let core_listener = UnixListener::bind(&core_path).expect("core fixture socket");
        let front_listener = UnixListener::bind(&front_path).expect("front fixture socket");

        let core = std::thread::spawn(move || {
            use capsule_nexus_wire::public_call::{Input, Output};
            use signal::{Answer, Contracted, Delivery, Dispatch};
            let (mut stream, _) = core_listener.accept().expect("core connection");
            let capacity = FrameCapacity::from(MAXIMUM_SIGNAL_BYTES);
            let greeting = Signal::<Dispatch<Input>>::from(Vec::from(
                stream.read_frame(capacity).expect("greeting"),
            ))
            .restore()
            .expect("restore greeting");
            let Dispatch::Greet(greeting) = greeting else {
                panic!("the translator greets first");
            };
            stream
                .write_frame(
                    &Delivery::<Output>::Greeted(Input::receipt(&greeting))
                        .signalize()
                        .expect("receipt signal"),
                    capacity,
                )
                .expect("receipt");
            let opened = Signal::<Dispatch<Input>>::from(Vec::from(
                stream.read_frame(capacity).expect("typed public Call"),
            ))
            .restore()
            .expect("restore public Call");
            let Dispatch::Open(opening) = opened else {
                panic!("the translator opens an exchange");
            };
            assert!(matches!(
                opening.query,
                Input::Call(ref data)
                    if matches!(data.public_input.0, signal_capsule_nexus::Input::Start(_))
            ));
            std::thread::sleep(Duration::from_millis(75));
            let response = Delivery::Answer(Answer {
                exchange: opening.exchange,
                response: Output::Answered(capsule_nexus_wire::public_payload::Output(
                    signal_capsule_nexus::Output::Started(signal_capsule_nexus::Outcome::Done),
                )),
            })
            .signalize()
            .expect("response signal");
            stream
                .write_frame(&response, capacity)
                .expect("delayed response");
            stream
                .write_frame(
                    &Delivery::<Output>::End(signal::Ending::completed(opening.exchange))
                        .signalize()
                        .expect("end signal"),
                    capacity,
                )
                .expect("the exchange's end");
        });

        let translator = CapsuleTranslator::from(ValidTextBudget::MAXIMUM);
        let configuration = &translator.configuration;
        let _ = configuration.apply(Set::HerdrPath(PathValue("/bin/true".into())));
        let _ = configuration.apply(Set::CallTimeout(Timeout(20)));
        let _ = configuration.apply(Set::OutputLimit(ByteLimit(4096)));
        let _ = configuration.apply(Set::TextBudget(BudgetValue {
            node_limit: NodeLimit(4096),
            depth_limit: DepthLimit(32),
        }));
        assert!(configuration.ready().is_some());
        let place = place(&directory.to_string_lossy());
        let front = std::thread::spawn(move || {
            let (mut stream, _) = front_listener.accept().expect("front connection");
            Connection {
                translator: &translator,
                place: &place,
                endpoint: Endpoint::Public,
                admission: admission(),
                bootstrap_budget: ValidTextBudget {
                    nodes: 4096,
                    depth: 32,
                },
                nexus: None,
                subscriptions: &MetaSubscriptions::default(),
            }
            .serve(&mut stream)
            .expect("front request");
        });

        let began = Instant::now();
        let answer = TextClient {
            socket: front_path.clone(),
            lane: TextLane::PublicText,
            admission_limit: MAXIMUM_SIGNAL_BYTES,
        }
        .request("Start.{ { s p } { a Codex Configured [] } label }")
        .expect("Start answer");
        assert!(began.elapsed() > Duration::from_millis(20));
        assert_eq!(answer, "Started.Done");
        front.join().expect("front thread");
        core.join().expect("core thread");
        std::fs::remove_dir_all(directory).expect("fixture cleanup");
    }
}
