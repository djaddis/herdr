use crate::terminal::TerminalId;

/// Viewport state for a pane.
///
/// Terminal identity, cwd, labels, and agent metadata live in TerminalState.
pub struct PaneState {
    pub attached_terminal_id: TerminalId,
    /// Whether the user has seen this pane since its last state change to Idle.
    /// False = "Done" (agent finished while user was in another workspace).
    pub seen: bool,
    /// Cap this pane's terminal content width at ui.readable_width_columns, centered.
    pub readable_width: bool,
    /// True when readable_width was auto-enabled by agent detection.
    pub readable_width_auto: bool,
}

impl PaneState {
    pub fn new(attached_terminal_id: TerminalId) -> Self {
        Self {
            attached_terminal_id,
            seen: true,
            readable_width: false,
            readable_width_auto: false,
        }
    }
}
