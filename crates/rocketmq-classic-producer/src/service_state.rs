use rocketmq_classic_protocol::codes::JavaEnumLookupError;

/// Lifecycle values exposed by Java's `DefaultMQProducerImpl` accessors.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ProducerServiceState {
    CreateJust,
    Running,
    ShutdownAlready,
    StartFailed,
}

impl ProducerServiceState {
    pub const fn values() -> [Self; 4] {
        [
            Self::CreateJust,
            Self::Running,
            Self::ShutdownAlready,
            Self::StartFailed,
        ]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
        match name {
            None => Err(JavaEnumLookupError::NullName),
            Some("CREATE_JUST") => Ok(Self::CreateJust),
            Some("RUNNING") => Ok(Self::Running),
            Some("SHUTDOWN_ALREADY") => Ok(Self::ShutdownAlready),
            Some("START_FAILED") => Ok(Self::StartFailed),
            Some(name) => Err(JavaEnumLookupError::UnknownName {
                enum_class: "org.apache.rocketmq.common.ServiceState",
                name: name.to_owned(),
            }),
        }
    }

    pub const fn name(self) -> &'static str {
        self.as_java_name()
    }

    pub const fn as_java_name(self) -> &'static str {
        match self {
            Self::CreateJust => "CREATE_JUST",
            Self::Running => "RUNNING",
            Self::ShutdownAlready => "SHUTDOWN_ALREADY",
            Self::StartFailed => "START_FAILED",
        }
    }

    pub const fn ordinal(self) -> i32 {
        match self {
            Self::CreateJust => 0,
            Self::Running => 1,
            Self::ShutdownAlready => 2,
            Self::StartFailed => 3,
        }
    }

    pub const fn java_compare_to(self, other: Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}
