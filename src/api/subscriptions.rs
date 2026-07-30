use regex::Regex;

use crate::api::schema::{
    ErrorBody, ErrorResponse, Method, PaneAgentStatusChangedEvent, PaneOutputMatchedEvent, Request,
    Subscription, SubscriptionEventData, SubscriptionEventEnvelope, SubscriptionEventKind,
};
use crate::api::server::{dispatch_to_app_with_timeout, APP_RESPONSE_TIMEOUT};
use crate::api::{ApiRequestSender, EventHub};

pub(super) fn output_match_read_source(
    source: &crate::api::schema::ReadSource,
) -> crate::api::schema::ReadSource {
    match source {
        crate::api::schema::ReadSource::Recent => crate::api::schema::ReadSource::RecentUnwrapped,
        other => *other,
    }
}

pub(super) fn match_output(
    text: &str,
    matcher: &crate::api::schema::OutputMatch,
    regex: Option<&Regex>,
) -> Option<String> {
    match matcher {
        crate::api::schema::OutputMatch::Substring { value } => text
            .lines()
            .find(|line| line.contains(value))
            .map(|line| line.to_string()),
        crate::api::schema::OutputMatch::Regex { .. } => regex.and_then(|re| {
            text.lines()
                .find(|line| re.is_match(line))
                .map(|line| line.to_string())
        }),
    }
}

pub(super) struct ActiveOutputMatchedSubscription {
    pane_id: String,
    source: crate::api::schema::ReadSource,
    lines: Option<u32>,
    matcher: crate::api::schema::OutputMatch,
    regex: Option<Regex>,
    strip_ansi: bool,
    currently_matching: bool,
    request_prefix: String,
}

pub(super) struct ActiveAgentStatusChangedSubscription {
    pane_id: String,
    status_filter: Option<crate::api::schema::AgentStatus>,
    last_status: Option<crate::api::schema::AgentStatus>,
    last_presentation: Option<PanePresentationSnapshot>,
    last_sequence: u64,
    initial_event: Option<PaneAgentStatusChangedEvent>,
    wait_semantics: bool,
    request_prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PanePresentationSnapshot {
    title: Option<String>,
    display_agent: Option<String>,
    custom_status: Option<String>,
    state_labels: std::collections::HashMap<String, String>,
}

impl PanePresentationSnapshot {
    fn from(pane: &crate::api::schema::PaneInfo) -> Self {
        Self {
            title: pane.title.clone(),
            display_agent: pane.display_agent.clone(),
            custom_status: pane.custom_status.clone(),
            state_labels: pane.state_labels.clone(),
        }
    }

    fn from_event(
        title: &Option<String>,
        display_agent: &Option<String>,
        custom_status: &Option<String>,
        state_labels: &std::collections::HashMap<String, String>,
    ) -> Self {
        Self {
            title: title.clone(),
            display_agent: display_agent.clone(),
            custom_status: custom_status.clone(),
            state_labels: state_labels.clone(),
        }
    }
}

/// Whether a status filter asking for `wanted` is satisfied by seeing `current`.
///
/// `events.subscribe` filters keep exact matching so streaming consumers can
/// partition the lifecycle by status. `events.wait` callers ask "is the agent
/// in this state", so completion equivalence applies: `done` is a UI attention
/// state (`idle` + not yet seen by the user), meaning a completion surfaces as
/// `idle` when the pane is being watched, and a `done` pane silently decays to
/// `idle` once viewed. Waiting for `idle` means "not running", which `done`
/// also satisfies; waiting for `done` means "a run completed", which an `idle`
/// reached from `working`/`blocked` also satisfies.
fn status_filter_satisfied(
    wait_semantics: bool,
    wanted: crate::api::schema::AgentStatus,
    current: crate::api::schema::AgentStatus,
    previous: Option<crate::api::schema::AgentStatus>,
) -> bool {
    if wait_semantics {
        wait_status_satisfied(wanted, current, previous)
    } else {
        wanted == current
    }
}

fn wait_status_satisfied(
    wanted: crate::api::schema::AgentStatus,
    current: crate::api::schema::AgentStatus,
    previous: Option<crate::api::schema::AgentStatus>,
) -> bool {
    use crate::api::schema::AgentStatus;
    match (wanted, current) {
        _ if wanted == current => true,
        (AgentStatus::Idle, AgentStatus::Done) => true,
        (AgentStatus::Done, AgentStatus::Idle) => {
            matches!(previous, Some(AgentStatus::Working | AgentStatus::Blocked))
        }
        _ => false,
    }
}

pub(super) struct ActiveEventSubscription {
    event_kind: crate::api::schema::EventKind,
    last_sequence: u64,
}

pub(super) enum ActiveSubscription {
    Event(ActiveEventSubscription),
    OutputMatched(ActiveOutputMatchedSubscription),
    AgentStatusChanged(Box<ActiveAgentStatusChangedSubscription>),
}

impl ActiveSubscription {
    pub(super) fn new(
        subscription: Subscription,
        request_id: &str,
        index: usize,
        api_tx: &ApiRequestSender,
        event_hub: &EventHub,
        wait_semantics: bool,
    ) -> Result<Self, ErrorResponse> {
        match subscription {
            Subscription::WorkspaceCreated {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::WorkspaceCreated,
                last_sequence: 0,
            })),
            Subscription::WorkspaceUpdated {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::WorkspaceUpdated,
                last_sequence: 0,
            })),
            Subscription::WorkspaceRenamed {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::WorkspaceRenamed,
                last_sequence: 0,
            })),
            Subscription::WorkspaceMoved {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::WorkspaceMoved,
                last_sequence: 0,
            })),
            Subscription::WorkspaceClosed {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::WorkspaceClosed,
                last_sequence: 0,
            })),
            Subscription::WorkspaceFocused {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::WorkspaceFocused,
                last_sequence: 0,
            })),
            Subscription::WorktreeCreated {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::WorktreeCreated,
                last_sequence: 0,
            })),
            Subscription::WorktreeOpened {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::WorktreeOpened,
                last_sequence: 0,
            })),
            Subscription::WorktreeRemoved {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::WorktreeRemoved,
                last_sequence: 0,
            })),
            Subscription::TabCreated {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::TabCreated,
                last_sequence: 0,
            })),
            Subscription::TabClosed {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::TabClosed,
                last_sequence: 0,
            })),
            Subscription::TabFocused {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::TabFocused,
                last_sequence: 0,
            })),
            Subscription::TabRenamed {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::TabRenamed,
                last_sequence: 0,
            })),
            Subscription::TabMoved {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::TabMoved,
                last_sequence: 0,
            })),
            Subscription::PaneCreated {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::PaneCreated,
                last_sequence: 0,
            })),
            Subscription::PaneClosed {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::PaneClosed,
                last_sequence: 0,
            })),
            Subscription::PaneFocused {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::PaneFocused,
                last_sequence: 0,
            })),
            Subscription::PaneMoved {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::PaneMoved,
                last_sequence: 0,
            })),
            Subscription::PaneExited {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::PaneExited,
                last_sequence: 0,
            })),
            Subscription::PaneAgentDetected {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::PaneAgentDetected,
                last_sequence: 0,
            })),
            Subscription::LayoutUpdated {} => Ok(Self::Event(ActiveEventSubscription {
                event_kind: crate::api::schema::EventKind::LayoutUpdated,
                last_sequence: 0,
            })),
            Subscription::PaneOutputMatched {
                pane_id,
                source,
                lines,
                r#match,
                strip_ansi,
            } => {
                let regex = match &r#match {
                    crate::api::schema::OutputMatch::Regex { value } => match Regex::new(value) {
                        Ok(regex) => Some(regex),
                        Err(err) => {
                            return Err(ErrorResponse {
                                id: request_id.to_string(),
                                error: ErrorBody {
                                    code: "invalid_regex".into(),
                                    message: err.to_string(),
                                },
                            });
                        }
                    },
                    crate::api::schema::OutputMatch::Substring { .. } => None,
                };

                let probe = pane_read(
                    format!("{request_id}:sub:{index}:probe"),
                    &pane_id,
                    source,
                    lines,
                    strip_ansi,
                    api_tx,
                );
                probe?;

                Ok(Self::OutputMatched(ActiveOutputMatchedSubscription {
                    pane_id,
                    source,
                    lines,
                    matcher: r#match,
                    regex,
                    strip_ansi,
                    currently_matching: false,
                    request_prefix: format!("{request_id}:sub:{index}"),
                }))
            }
            Subscription::PaneAgentStatusChanged {
                pane_id,
                agent_status,
            } => {
                let last_sequence = event_hub.current_sequence();
                let probe = pane_get(format!("{request_id}:sub:{index}:probe"), &pane_id, api_tx)?;
                let last_status = probe.agent_status;
                let last_presentation = PanePresentationSnapshot::from(&probe);
                let initial_event = agent_status
                    .is_some_and(|wanted| {
                        status_filter_satisfied(wait_semantics, wanted, probe.agent_status, None)
                    })
                    .then_some(PaneAgentStatusChangedEvent {
                        pane_id: probe.pane_id.clone(),
                        workspace_id: probe.workspace_id,
                        agent_status: probe.agent_status,
                        agent: probe.agent,
                        title: probe.title,
                        display_agent: probe.display_agent,
                        custom_status: probe.custom_status,
                        state_labels: probe.state_labels,
                    });

                Ok(Self::AgentStatusChanged(Box::new(
                    ActiveAgentStatusChangedSubscription {
                        pane_id: probe.pane_id,
                        status_filter: agent_status,
                        last_status: Some(last_status),
                        last_presentation: Some(last_presentation),
                        last_sequence,
                        initial_event,
                        wait_semantics,
                        request_prefix: format!("{request_id}:sub:{index}"),
                    },
                )))
            }
        }
    }

    pub(super) fn poll(
        &mut self,
        api_tx: &ApiRequestSender,
        event_hub: &EventHub,
    ) -> Option<serde_json::Value> {
        match self {
            Self::Event(subscription) => subscription.poll(event_hub),
            Self::OutputMatched(subscription) => {
                serde_json::to_value(subscription.poll(api_tx)?).ok()
            }
            Self::AgentStatusChanged(subscription) => {
                serde_json::to_value(subscription.poll(api_tx, event_hub)?).ok()
            }
        }
    }
}

impl ActiveEventSubscription {
    fn poll(&mut self, event_hub: &EventHub) -> Option<serde_json::Value> {
        for (sequence, event) in event_hub.events_after(self.last_sequence) {
            self.last_sequence = sequence;
            if event.event == self.event_kind {
                return serde_json::to_value(event).ok();
            }
        }
        None
    }
}

impl ActiveOutputMatchedSubscription {
    fn poll(&mut self, api_tx: &ApiRequestSender) -> Option<SubscriptionEventEnvelope> {
        let read = pane_read(
            format!("{}:read", self.request_prefix),
            &self.pane_id,
            output_match_read_source(&self.source),
            self.lines,
            self.strip_ansi,
            api_tx,
        )
        .ok()?;

        let matched_line = match_output(&read.text, &self.matcher, self.regex.as_ref());
        match matched_line {
            Some(matched_line) => {
                if self.currently_matching {
                    return None;
                }
                self.currently_matching = true;
                Some(SubscriptionEventEnvelope {
                    event: SubscriptionEventKind::PaneOutputMatched,
                    data: SubscriptionEventData::PaneOutputMatched(PaneOutputMatchedEvent {
                        pane_id: read.pane_id.clone(),
                        matched_line,
                        read,
                    }),
                })
            }
            None => {
                self.currently_matching = false;
                None
            }
        }
    }
}

impl ActiveAgentStatusChangedSubscription {
    fn poll(
        &mut self,
        api_tx: &ApiRequestSender,
        event_hub: &EventHub,
    ) -> Option<SubscriptionEventEnvelope> {
        let mut saw_status_event = false;
        for (sequence, event) in event_hub.events_after(self.last_sequence) {
            self.last_sequence = sequence;
            let crate::api::schema::EventData::PaneAgentStatusChanged {
                pane_id,
                workspace_id,
                agent_status,
                previous_agent_status,
                agent,
                title,
                display_agent,
                custom_status,
                state_labels,
            } = event.data
            else {
                continue;
            };
            if event.event != crate::api::schema::EventKind::PaneAgentStatusChanged {
                continue;
            }
            if pane_id != self.pane_id {
                continue;
            }
            saw_status_event = true;

            let current_presentation = PanePresentationSnapshot::from_event(
                &title,
                &display_agent,
                &custom_status,
                &state_labels,
            );
            let tracked_previous = self.last_status.replace(agent_status);
            self.last_presentation = Some(current_presentation);
            let previous_status = previous_agent_status.or(tracked_previous);
            if self.status_filter.is_some_and(|wanted| {
                !status_filter_satisfied(self.wait_semantics, wanted, agent_status, previous_status)
            }) {
                continue;
            }

            self.initial_event = None;
            return Some(SubscriptionEventEnvelope {
                event: SubscriptionEventKind::PaneAgentStatusChanged,
                data: SubscriptionEventData::PaneAgentStatusChanged(PaneAgentStatusChangedEvent {
                    pane_id,
                    workspace_id,
                    agent_status,
                    agent,
                    title,
                    display_agent,
                    custom_status,
                    state_labels,
                }),
            });
        }

        if saw_status_event {
            self.initial_event = None;
        } else if event_hub.current_sequence() != self.last_sequence {
            return None;
        } else if let Some(event) = self.initial_event.take() {
            return Some(SubscriptionEventEnvelope {
                event: SubscriptionEventKind::PaneAgentStatusChanged,
                data: SubscriptionEventData::PaneAgentStatusChanged(event),
            });
        }

        let before_snapshot_sequence = self.last_sequence;
        let pane = pane_get(
            format!("{}:pane", self.request_prefix),
            &self.pane_id,
            api_tx,
        )
        .ok()?;
        let after_snapshot_sequence = event_hub.current_sequence();
        if after_snapshot_sequence != before_snapshot_sequence {
            return None;
        }

        let event = self.event_from_snapshot(pane);
        if event.is_some() {
            self.last_sequence = after_snapshot_sequence;
        }
        event
    }

    fn event_from_snapshot(
        &mut self,
        pane: crate::api::schema::PaneInfo,
    ) -> Option<SubscriptionEventEnvelope> {
        let current_status = pane.agent_status;
        let current_presentation = PanePresentationSnapshot::from(&pane);
        let previous_status = self.last_status.replace(current_status);
        let previous_presentation = self.last_presentation.replace(current_presentation.clone());
        let presentation_changed = previous_presentation
            .as_ref()
            .is_some_and(|previous| previous != &current_presentation);
        let status_changed = previous_status.is_some_and(|previous| previous != current_status);
        if !(status_changed || presentation_changed) {
            return None;
        }
        if self.status_filter.is_some_and(|wanted| {
            !status_filter_satisfied(self.wait_semantics, wanted, current_status, previous_status)
        }) {
            return None;
        }

        Some(SubscriptionEventEnvelope {
            event: SubscriptionEventKind::PaneAgentStatusChanged,
            data: SubscriptionEventData::PaneAgentStatusChanged(PaneAgentStatusChangedEvent {
                pane_id: pane.pane_id,
                workspace_id: pane.workspace_id,
                agent_status: current_status,
                agent: pane.agent,
                title: pane.title,
                display_agent: pane.display_agent,
                custom_status: pane.custom_status,
                state_labels: pane.state_labels,
            }),
        })
    }
}

fn pane_read(
    request_id: String,
    pane_id: &str,
    source: crate::api::schema::ReadSource,
    lines: Option<u32>,
    strip_ansi: bool,
    api_tx: &ApiRequestSender,
) -> Result<crate::api::schema::PaneReadResult, ErrorResponse> {
    let response = dispatch_to_app_with_timeout(
        Request {
            id: request_id.clone(),
            method: Method::PaneRead(crate::api::schema::PaneReadParams {
                pane_id: pane_id.to_string(),
                source,
                lines,
                format: crate::api::schema::ReadFormat::Text,
                strip_ansi,
            }),
        },
        api_tx,
        Some(APP_RESPONSE_TIMEOUT),
    );
    let value: serde_json::Value = serde_json::from_str(&response).map_err(|_| ErrorResponse {
        id: request_id.clone(),
        error: ErrorBody {
            code: "internal_error".into(),
            message: "failed to decode pane read response".into(),
        },
    })?;
    if value.get("error").is_some() {
        return serde_json::from_value(value).map_err(|_| ErrorResponse {
            id: request_id,
            error: ErrorBody {
                code: "internal_error".into(),
                message: "failed to decode pane read error".into(),
            },
        });
    }
    serde_json::from_value(value["result"]["read"].clone()).map_err(|_| ErrorResponse {
        id: request_id,
        error: ErrorBody {
            code: "internal_error".into(),
            message: "failed to decode pane read result".into(),
        },
    })
}

fn pane_get(
    request_id: String,
    pane_id: &str,
    api_tx: &ApiRequestSender,
) -> Result<crate::api::schema::PaneInfo, ErrorResponse> {
    let response = dispatch_to_app_with_timeout(
        Request {
            id: request_id.clone(),
            method: Method::PaneGet(crate::api::schema::PaneTarget {
                pane_id: pane_id.to_string(),
            }),
        },
        api_tx,
        Some(APP_RESPONSE_TIMEOUT),
    );
    let value: serde_json::Value = serde_json::from_str(&response).map_err(|_| ErrorResponse {
        id: request_id.clone(),
        error: ErrorBody {
            code: "internal_error".into(),
            message: "failed to decode pane get response".into(),
        },
    })?;
    if value.get("error").is_some() {
        return serde_json::from_value(value).map_err(|_| ErrorResponse {
            id: request_id,
            error: ErrorBody {
                code: "internal_error".into(),
                message: "failed to decode pane get error".into(),
            },
        });
    }
    serde_json::from_value(value["result"]["pane"].clone()).map_err(|_| ErrorResponse {
        id: request_id,
        error: ErrorBody {
            code: "internal_error".into(),
            message: "failed to decode pane get result".into(),
        },
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::api::schema::{AgentStatus, EventData, EventEnvelope, EventKind};

    fn status_event(custom_status: Option<&str>) -> EventEnvelope {
        EventEnvelope {
            event: EventKind::PaneAgentStatusChanged,
            data: EventData::PaneAgentStatusChanged {
                pane_id: "pane_1".into(),
                workspace_id: "workspace_1".into(),
                agent_status: AgentStatus::Working,
                previous_agent_status: None,
                agent: Some("pi".into()),
                title: None,
                display_agent: None,
                custom_status: custom_status.map(str::to_string),
                state_labels: HashMap::new(),
            },
        }
    }

    fn agent_status_change_event(
        agent_status: AgentStatus,
        previous_agent_status: Option<AgentStatus>,
    ) -> EventEnvelope {
        EventEnvelope {
            event: EventKind::PaneAgentStatusChanged,
            data: EventData::PaneAgentStatusChanged {
                pane_id: "pane_1".into(),
                workspace_id: "workspace_1".into(),
                agent_status,
                previous_agent_status,
                agent: Some("omp".into()),
                title: None,
                display_agent: None,
                custom_status: None,
                state_labels: HashMap::new(),
            },
        }
    }

    fn subscription_with_filter(
        event_hub: &EventHub,
        status_filter: Option<AgentStatus>,
        last_status: AgentStatus,
        wait_semantics: bool,
    ) -> ActiveAgentStatusChangedSubscription {
        ActiveAgentStatusChangedSubscription {
            pane_id: "pane_1".into(),
            status_filter,
            last_status: Some(last_status),
            last_presentation: Some(PanePresentationSnapshot {
                title: None,
                display_agent: None,
                custom_status: None,
                state_labels: HashMap::new(),
            }),
            last_sequence: event_hub.current_sequence(),
            initial_event: None,
            wait_semantics,
            request_prefix: "test".into(),
        }
    }

    fn polled_status(
        subscription: &mut ActiveAgentStatusChangedSubscription,
        event_hub: &EventHub,
    ) -> Option<AgentStatus> {
        let event = subscription.poll(&tokio::sync::mpsc::unbounded_channel().0, event_hub)?;
        let SubscriptionEventData::PaneAgentStatusChanged(data) = event.data else {
            panic!("wrong event data");
        };
        Some(data.agent_status)
    }

    #[test]
    fn wait_status_satisfied_treats_done_as_idle_and_completion_as_done() {
        assert!(wait_status_satisfied(
            AgentStatus::Idle,
            AgentStatus::Done,
            None
        ));
        assert!(wait_status_satisfied(
            AgentStatus::Done,
            AgentStatus::Idle,
            Some(AgentStatus::Working)
        ));
        assert!(wait_status_satisfied(
            AgentStatus::Done,
            AgentStatus::Idle,
            Some(AgentStatus::Blocked)
        ));
        assert!(!wait_status_satisfied(
            AgentStatus::Done,
            AgentStatus::Idle,
            None
        ));
        assert!(!wait_status_satisfied(
            AgentStatus::Done,
            AgentStatus::Idle,
            Some(AgentStatus::Done)
        ));
        assert!(!wait_status_satisfied(
            AgentStatus::Working,
            AgentStatus::Idle,
            Some(AgentStatus::Working)
        ));
    }

    #[test]
    fn done_filter_matches_completion_that_lands_on_idle() {
        // A watched pane is marked seen on completion, so the event stream
        // reports working -> idle instead of working -> done. A waiter for
        // done must still treat that edge as completion.
        let event_hub = EventHub::default();
        let mut subscription = subscription_with_filter(
            &event_hub,
            Some(AgentStatus::Done),
            AgentStatus::Working,
            true,
        );

        event_hub.push(agent_status_change_event(
            AgentStatus::Idle,
            Some(AgentStatus::Working),
        ));

        assert_eq!(
            polled_status(&mut subscription, &event_hub),
            Some(AgentStatus::Idle)
        );
    }

    #[test]
    fn done_filter_ignores_done_to_idle_decay() {
        // Viewing a done pane silently turns it idle; that is not a new
        // completion and must not wake a done waiter. Only the later real
        // completion (working -> idle) may match.
        let event_hub = EventHub::default();
        let mut subscription =
            subscription_with_filter(&event_hub, Some(AgentStatus::Done), AgentStatus::Done, true);

        let mut decay_event = agent_status_change_event(AgentStatus::Idle, Some(AgentStatus::Done));
        let EventData::PaneAgentStatusChanged { custom_status, .. } = &mut decay_event.data else {
            panic!("wrong event data");
        };
        *custom_status = Some("decay".into());
        event_hub.push(decay_event);
        event_hub.push(agent_status_change_event(
            AgentStatus::Working,
            Some(AgentStatus::Idle),
        ));
        event_hub.push(agent_status_change_event(
            AgentStatus::Idle,
            Some(AgentStatus::Working),
        ));

        let event = subscription
            .poll(&tokio::sync::mpsc::unbounded_channel().0, &event_hub)
            .expect("completion event");
        let SubscriptionEventData::PaneAgentStatusChanged(data) = event.data else {
            panic!("wrong event data");
        };
        assert_eq!(data.agent_status, AgentStatus::Idle);
        assert_eq!(data.custom_status, None, "decay event must not match");
    }

    #[test]
    fn idle_filter_matches_done_event() {
        let event_hub = EventHub::default();
        let mut subscription = subscription_with_filter(
            &event_hub,
            Some(AgentStatus::Idle),
            AgentStatus::Working,
            true,
        );

        event_hub.push(agent_status_change_event(
            AgentStatus::Done,
            Some(AgentStatus::Working),
        ));

        assert_eq!(
            polled_status(&mut subscription, &event_hub),
            Some(AgentStatus::Done)
        );
    }

    #[test]
    fn done_filter_matches_completion_replayed_before_probe() {
        // A completion can land between the sequence cursor capture and the
        // setup probe, so the probe already reports the post-completion
        // status. The replayed event carries its own predecessor and must
        // still match a done wait.
        let event_hub = EventHub::default();
        let mut subscription =
            subscription_with_filter(&event_hub, Some(AgentStatus::Done), AgentStatus::Idle, true);

        event_hub.push(agent_status_change_event(
            AgentStatus::Idle,
            Some(AgentStatus::Working),
        ));

        assert_eq!(
            polled_status(&mut subscription, &event_hub),
            Some(AgentStatus::Idle)
        );
    }

    #[test]
    fn subscribe_status_filters_stay_exact() {
        // events.subscribe consumers partition the lifecycle by exact status;
        // wait equivalence must not leak into that contract.
        let event_hub = EventHub::default();
        let mut subscription = subscription_with_filter(
            &event_hub,
            Some(AgentStatus::Idle),
            AgentStatus::Working,
            false,
        );

        event_hub.push(agent_status_change_event(
            AgentStatus::Done,
            Some(AgentStatus::Working),
        ));
        event_hub.push(agent_status_change_event(
            AgentStatus::Idle,
            Some(AgentStatus::Done),
        ));

        assert_eq!(
            polled_status(&mut subscription, &event_hub),
            Some(AgentStatus::Idle),
            "done event must not satisfy an exact idle subscription"
        );
    }

    #[test]
    fn agent_status_subscription_replays_queued_metadata_set_and_expiry_events() {
        let event_hub = EventHub::default();
        let mut subscription = ActiveAgentStatusChangedSubscription {
            pane_id: "pane_1".into(),
            status_filter: None,
            last_status: Some(AgentStatus::Working),
            last_presentation: Some(PanePresentationSnapshot {
                title: None,
                display_agent: None,
                custom_status: None,
                state_labels: HashMap::new(),
            }),
            last_sequence: event_hub.current_sequence(),
            initial_event: None,
            wait_semantics: false,
            request_prefix: "test".into(),
        };

        event_hub.push(status_event(Some("short lived")));
        event_hub.push(status_event(None));

        let set_event = subscription
            .poll(&tokio::sync::mpsc::unbounded_channel().0, &event_hub)
            .expect("set event");
        let SubscriptionEventData::PaneAgentStatusChanged(set_data) = set_event.data else {
            panic!("wrong event data");
        };
        assert_eq!(set_data.custom_status.as_deref(), Some("short lived"));

        let expiry_event = subscription
            .poll(&tokio::sync::mpsc::unbounded_channel().0, &event_hub)
            .expect("expiry event");
        let SubscriptionEventData::PaneAgentStatusChanged(expiry_data) = expiry_event.data else {
            panic!("wrong event data");
        };
        assert_eq!(expiry_data.custom_status, None);
    }

    #[test]
    fn agent_status_subscription_prefers_setup_window_events_over_initial_snapshot() {
        let event_hub = EventHub::default();
        let mut subscription = ActiveAgentStatusChangedSubscription {
            pane_id: "pane_1".into(),
            status_filter: Some(AgentStatus::Working),
            last_status: Some(AgentStatus::Working),
            last_presentation: Some(PanePresentationSnapshot {
                title: None,
                display_agent: None,
                custom_status: None,
                state_labels: HashMap::new(),
            }),
            last_sequence: event_hub.current_sequence(),
            initial_event: Some(PaneAgentStatusChangedEvent {
                pane_id: "pane_1".into(),
                workspace_id: "workspace_1".into(),
                agent_status: AgentStatus::Working,
                agent: Some("pi".into()),
                title: None,
                display_agent: None,
                custom_status: None,
                state_labels: HashMap::new(),
            }),
            wait_semantics: false,
            request_prefix: "test".into(),
        };

        event_hub.push(status_event(Some("short lived")));
        event_hub.push(status_event(None));

        let set_event = subscription
            .poll(&tokio::sync::mpsc::unbounded_channel().0, &event_hub)
            .expect("set event");
        let SubscriptionEventData::PaneAgentStatusChanged(set_data) = set_event.data else {
            panic!("wrong event data");
        };
        assert_eq!(set_data.custom_status.as_deref(), Some("short lived"));

        let expiry_event = subscription
            .poll(&tokio::sync::mpsc::unbounded_channel().0, &event_hub)
            .expect("expiry event");
        let SubscriptionEventData::PaneAgentStatusChanged(expiry_data) = expiry_event.data else {
            panic!("wrong event data");
        };
        assert_eq!(expiry_data.custom_status, None);
    }

    #[test]
    fn agent_status_subscription_emits_setup_window_event_already_reflected_by_probe() {
        let event_hub = EventHub::default();
        let mut subscription = ActiveAgentStatusChangedSubscription {
            pane_id: "pane_1".into(),
            status_filter: Some(AgentStatus::Working),
            last_status: Some(AgentStatus::Working),
            last_presentation: Some(PanePresentationSnapshot {
                title: None,
                display_agent: None,
                custom_status: Some("short lived".into()),
                state_labels: HashMap::new(),
            }),
            last_sequence: event_hub.current_sequence(),
            initial_event: Some(PaneAgentStatusChangedEvent {
                pane_id: "pane_1".into(),
                workspace_id: "workspace_1".into(),
                agent_status: AgentStatus::Working,
                agent: Some("pi".into()),
                title: None,
                display_agent: None,
                custom_status: Some("short lived".into()),
                state_labels: HashMap::new(),
            }),
            wait_semantics: false,
            request_prefix: "test".into(),
        };

        event_hub.push(status_event(Some("short lived")));

        let event = subscription
            .poll(&tokio::sync::mpsc::unbounded_channel().0, &event_hub)
            .expect("setup-window event");
        let SubscriptionEventData::PaneAgentStatusChanged(data) = event.data else {
            panic!("wrong event data");
        };
        assert_eq!(data.custom_status.as_deref(), Some("short lived"));
        assert!(subscription.initial_event.is_none());
    }
}
