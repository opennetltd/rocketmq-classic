/// Lifecycle values exposed by RocketMQ consumer implementation accessors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerServiceState {
    CreateJust,
    StartFailed,
    Running,
    ShutdownAlready,
}

impl ConsumerServiceState {
    pub const fn as_java_name(self) -> &'static str {
        match self {
            Self::CreateJust => "CREATE_JUST",
            Self::StartFailed => "START_FAILED",
            Self::Running => "RUNNING",
            Self::ShutdownAlready => "SHUTDOWN_ALREADY",
        }
    }
}
