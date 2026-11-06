use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use super::{RuntimeError, RuntimeScheduler};
use rocketmq_classic_protocol::body::ConsumerConsumeStatus;

const MINUTE_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
const HOUR_SAMPLE_INTERVAL: Duration = Duration::from_secs(10 * 60);
const DAY_SAMPLE_INTERVAL: Duration = Duration::from_secs(60 * 60);
const MINUTE_SAMPLE_COUNT: usize = 7;
const HOUR_SAMPLE_COUNT: usize = 7;
const DAY_SAMPLE_COUNT: usize = 25;

type StatsKey = String;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StatsLogWindow {
    Minute,
    Hour,
    Day,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StatsItemSetKind {
    ConsumeOkTps,
    ConsumeRt,
    ConsumeFailedTps,
    PullTps,
    PullRt,
}

impl StatsItemSetKind {
    pub(super) const ALL: [Self; 5] = [
        Self::ConsumeOkTps,
        Self::ConsumeRt,
        Self::ConsumeFailedTps,
        Self::PullTps,
        Self::PullRt,
    ];

    fn log_spec(self) -> (&'static str, bool) {
        match self {
            Self::ConsumeOkTps => ("CONSUME_OK_TPS", false),
            Self::ConsumeRt => ("CONSUME_RT", true),
            Self::ConsumeFailedTps => ("CONSUME_FAILED_TPS", false),
            Self::PullTps => ("PULL_TPS", false),
            Self::PullRt => ("PULL_RT", true),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum StatsScheduleKind {
    Sample(StatsItemSetKind),
    Print(StatsItemSetKind, StatsLogWindow),
}

#[derive(Clone, Copy)]
pub(super) struct StatsScheduleSpec {
    pub(super) kind: StatsScheduleKind,
    pub(super) delay: Duration,
    pub(super) period: Duration,
}

pub(super) fn stats_schedule_specs() -> Vec<StatsScheduleSpec> {
    let mut result = Vec::with_capacity(30);
    for set in StatsItemSetKind::ALL {
        for period in [
            Duration::from_secs(10),
            Duration::from_secs(10 * 60),
            Duration::from_secs(60 * 60),
        ] {
            result.push(StatsScheduleSpec {
                kind: StatsScheduleKind::Sample(set),
                delay: Duration::ZERO,
                period,
            });
        }

        for (window, boundary, period) in [
            (
                StatsLogWindow::Minute,
                LocalBoundary::Minute,
                Duration::from_secs(60),
            ),
            (
                StatsLogWindow::Hour,
                LocalBoundary::Hour,
                Duration::from_secs(60 * 60),
            ),
            (
                StatsLogWindow::Day,
                LocalBoundary::Day,
                Duration::from_secs(24 * 60 * 60),
            ),
        ] {
            result.push(StatsScheduleSpec {
                kind: StatsScheduleKind::Print(set, window),
                delay: next_local_boundary_delay(boundary),
                period,
            });
        }
    }
    result
}

#[derive(Clone, Copy)]
enum LocalBoundary {
    Minute,
    Hour,
    Day,
}

fn next_local_boundary_delay(boundary: LocalBoundary) -> Duration {
    use time::{Duration as CalendarDuration, OffsetDateTime, Time};

    let now = OffsetDateTime::now_local().unwrap_or_else(|_| OffsetDateTime::now_utc());
    let target = match boundary {
        LocalBoundary::Minute => now
            .replace_second(0)
            .ok()
            .and_then(|value| value.replace_nanosecond(0).ok())
            .and_then(|value| value.checked_add(CalendarDuration::minutes(1))),
        LocalBoundary::Hour => now
            .replace_minute(0)
            .ok()
            .and_then(|value| value.replace_second(0).ok())
            .and_then(|value| value.replace_nanosecond(0).ok())
            .and_then(|value| value.checked_add(CalendarDuration::hours(1))),
        LocalBoundary::Day => now
            .date()
            .next_day()
            .map(|date| date.with_time(Time::MIDNIGHT).assume_offset(now.offset())),
    };
    let Some(target) = target else {
        return Duration::from_secs(1);
    };
    let nanos = target.unix_timestamp_nanos() - now.unix_timestamp_nanos();
    Duration::from_nanos(nanos.clamp(1, u64::MAX as i128) as u64)
}

impl StatsLogWindow {
    fn label(self) -> &'static str {
        match self {
            Self::Minute => "One Minute",
            Self::Hour => "One Hour",
            Self::Day => "One Day",
        }
    }
}

/// Java 5.5.1 consumer metrics shared by one `MQClientInstance` equivalent.
///
/// Standalone construction requires a scheduler, as in Java:
/// [`ConsumerStatsManager::with_scheduler`]. Runtime ownership allocates its
/// shared instance internally before registering its scheduler jobs.
///
/// ```compile_fail
/// use rocketmq_classic_core::runtime::ConsumerStatsManager;
/// let _ = ConsumerStatsManager::new();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_core::runtime::ConsumerStatsManager;
/// let _ = ConsumerStatsManager::default();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_core::runtime::ConsumerStatsManager;
/// let _ = ConsumerStatsManager::new_for_runtime();
/// ```
pub struct ConsumerStatsManager {
    sampling_origin: Instant,
    topics: Mutex<HashMap<StatsKey, TopicStats>>,
}

impl ConsumerStatsManager {
    pub(super) fn new_for_runtime() -> Self {
        Self::with_sampling_origin(Instant::now())
    }

    /// Creates a manager and registers its Java-compatible maintenance jobs
    /// with the supplied client scheduler.
    pub fn with_scheduler(scheduler: &RuntimeScheduler) -> Result<Arc<Self>, RuntimeError> {
        let manager = Arc::new(Self::new_for_runtime());
        for spec in stats_schedule_specs() {
            match spec.kind {
                StatsScheduleKind::Sample(set) => {
                    let manager = manager.clone();
                    scheduler.schedule_at_fixed_rate(spec.delay, spec.period, move || {
                        let manager = manager.clone();
                        async move { manager.sample(set) }
                    })?;
                }
                StatsScheduleKind::Print(set, window) => {
                    let manager = manager.clone();
                    scheduler.schedule_at_fixed_rate(spec.delay, spec.period, move || {
                        let manager = manager.clone();
                        async move { manager.log(set, window) }
                    })?;
                }
            }
        }
        Ok(manager)
    }

    fn with_sampling_origin(sampling_origin: Instant) -> Self {
        Self {
            sampling_origin,
            topics: Mutex::new(HashMap::new()),
        }
    }

    /// Java's lifecycle hooks are no-ops; scheduled samples are evaluated lazily.
    pub fn start(&self) {}

    /// Java's lifecycle hooks are no-ops; the runtime owns this manager.
    pub fn shutdown(&self) {}

    pub(super) fn sample(&self, set: StatsItemSetKind) {
        self.sample_at(set, Instant::now());
    }

    fn sample_at(&self, set: StatsItemSetKind, now: Instant) {
        for stats in self.lock_topics().values_mut() {
            stats.series_mut(set).advance(now);
        }
    }

    pub(super) fn log(&self, set: StatsItemSetKind, window: StatsLogWindow) {
        for event in self.log_events(set, window) {
            event.emit();
        }
    }

    fn log_events(&self, set: StatsItemSetKind, window: StatsLogWindow) -> Vec<StatsLogEvent> {
        self.lock_topics()
            .iter()
            .filter_map(|(key, stats)| stats.log_event(key, set, window))
            .collect()
    }

    pub fn inc_pull_rt(&self, group: &str, topic: &str, rt: i64) {
        self.update_topic(group, topic, |stats, now| {
            stats.pull_rt.record(now, java_int(rt), 1);
        });
    }

    pub fn inc_pull_tps(&self, group: &str, topic: &str, messages: i64) {
        self.update_topic(group, topic, |stats, now| {
            stats.pull_tps.record(now, java_int(messages), 1);
        });
    }

    pub fn inc_consume_rt(&self, group: &str, topic: &str, rt: i64) {
        self.update_topic(group, topic, |stats, now| {
            stats.consume_rt.record(now, java_int(rt), 1);
        });
    }

    pub fn inc_consume_ok_tps(&self, group: &str, topic: &str, messages: i64) {
        self.update_topic(group, topic, |stats, now| {
            stats.consume_ok_tps.record(now, java_int(messages), 1);
        });
    }

    pub fn inc_consume_failed_tps(&self, group: &str, topic: &str, messages: i64) {
        self.update_topic(group, topic, |stats, now| {
            stats.consume_failed_tps.record(now, java_int(messages), 1);
        });
    }

    pub fn consume_status(&self, group: &str, topic: &str) -> ConsumerConsumeStatus {
        self.status_at(group, topic, Instant::now())
    }

    fn status_at(&self, group: &str, topic: &str, now: Instant) -> ConsumerConsumeStatus {
        let mut topics = self.lock_topics();
        topics
            .get_mut(&java_stats_key(group, topic))
            .map(|stats| stats.status(now))
            .unwrap_or_default()
    }

    fn update_topic(
        &self,
        group: &str,
        topic: &str,
        update: impl FnOnce(&mut TopicStats, Instant),
    ) {
        self.update_topic_at(group, topic, Instant::now(), update);
    }

    fn update_topic_at(
        &self,
        group: &str,
        topic: &str,
        now: Instant,
        update: impl FnOnce(&mut TopicStats, Instant),
    ) {
        let mut topics = self.lock_topics();
        let stats = topics
            .entry(java_stats_key(group, topic))
            .or_insert_with(|| TopicStats::new(self.sampling_origin));
        update(stats, now);
    }

    fn lock_topics(&self) -> std::sync::MutexGuard<'_, HashMap<StatsKey, TopicStats>> {
        self.topics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

struct TopicStats {
    pull_rt: MetricSeries,
    pull_tps: MetricSeries,
    consume_rt: MetricSeries,
    consume_ok_tps: MetricSeries,
    consume_failed_tps: MetricSeries,
}

impl TopicStats {
    fn new(sampling_origin: Instant) -> Self {
        Self {
            pull_rt: MetricSeries::new(sampling_origin),
            pull_tps: MetricSeries::new(sampling_origin),
            consume_rt: MetricSeries::new(sampling_origin),
            consume_ok_tps: MetricSeries::new(sampling_origin),
            consume_failed_tps: MetricSeries::new(sampling_origin),
        }
    }
}

impl TopicStats {
    fn status(&mut self, now: Instant) -> ConsumerConsumeStatus {
        self.advance(now);
        let minute_consume_rt = self.consume_rt.minute.average();
        let consume_rt = if minute_consume_rt.1 == 0 {
            self.consume_rt.hour.average().0
        } else {
            minute_consume_rt.0
        };
        let mut status = ConsumerConsumeStatus::default();
        status.set_pull_rt(self.pull_rt.minute.average().0);
        status.set_pull_tps(self.pull_tps.minute.rate());
        status.set_consume_rt(consume_rt);
        status.set_consume_ok_tps(self.consume_ok_tps.minute.rate());
        status.set_consume_failed_tps(self.consume_failed_tps.minute.rate());
        status.set_consume_failed_msgs(self.consume_failed_tps.hour.sum());
        status
    }

    fn advance(&mut self, now: Instant) {
        self.pull_rt.advance(now);
        self.pull_tps.advance(now);
        self.consume_rt.advance(now);
        self.consume_ok_tps.advance(now);
        self.consume_failed_tps.advance(now);
    }

    fn series(&self, set: StatsItemSetKind) -> &MetricSeries {
        match set {
            StatsItemSetKind::ConsumeOkTps => &self.consume_ok_tps,
            StatsItemSetKind::ConsumeRt => &self.consume_rt,
            StatsItemSetKind::ConsumeFailedTps => &self.consume_failed_tps,
            StatsItemSetKind::PullTps => &self.pull_tps,
            StatsItemSetKind::PullRt => &self.pull_rt,
        }
    }

    fn series_mut(&mut self, set: StatsItemSetKind) -> &mut MetricSeries {
        match set {
            StatsItemSetKind::ConsumeOkTps => &mut self.consume_ok_tps,
            StatsItemSetKind::ConsumeRt => &mut self.consume_rt,
            StatsItemSetKind::ConsumeFailedTps => &mut self.consume_failed_tps,
            StatsItemSetKind::PullTps => &mut self.pull_tps,
            StatsItemSetKind::PullRt => &mut self.pull_rt,
        }
    }

    fn log_event(
        &self,
        key: &str,
        set: StatsItemSetKind,
        window: StatsLogWindow,
    ) -> Option<StatsLogEvent> {
        let (name, response_time) = set.log_spec();
        self.series(set).log_event(name, key, window, response_time)
    }
}

struct MetricSeries {
    initialized: bool,
    value: i64,
    calls: i64,
    minute: SampleWindow,
    hour: SampleWindow,
    day: SampleWindow,
}

impl MetricSeries {
    fn new(sampling_origin: Instant) -> Self {
        Self {
            initialized: false,
            value: 0,
            calls: 0,
            minute: SampleWindow::minute(sampling_origin),
            hour: SampleWindow::hour(sampling_origin),
            day: SampleWindow::day(sampling_origin),
        }
    }

    fn record(&mut self, now: Instant, value: i64, calls: i64) {
        self.initialized = true;
        self.advance(now);
        self.minute.schedule_after(now);
        self.hour.schedule_after(now);
        self.day.schedule_after(now);
        self.value = self.value.wrapping_add(value);
        self.calls = self.calls.wrapping_add(calls);
    }

    fn advance(&mut self, now: Instant) {
        self.minute.sample_until(now, self.value, self.calls);
        self.hour.sample_until(now, self.value, self.calls);
        self.day.sample_until(now, self.value, self.calls);
    }

    fn log_event(
        &self,
        name: &'static str,
        key: &str,
        window: StatsLogWindow,
        response_time: bool,
    ) -> Option<StatsLogEvent> {
        if !self.initialized {
            return None;
        }
        let samples = match window {
            StatsLogWindow::Minute => &self.minute,
            StatsLogWindow::Hour => &self.hour,
            StatsLogWindow::Day => &self.day,
        };
        Some(StatsLogEvent {
            name,
            key: key.to_owned(),
            window: window.label(),
            response_time,
            summary: samples.summary(),
        })
    }
}

struct StatsLogEvent {
    name: &'static str,
    key: String,
    window: &'static str,
    response_time: bool,
    summary: MetricSummary,
}

impl StatsLogEvent {
    fn emit(self) {
        let Self {
            name,
            key,
            window,
            response_time,
            summary,
        } = self;
        if response_time {
            tracing::info!(
                target: "org.apache.rocketmq.client.stat.ConsumerStatsManager",
                "[{}] [{}] Stats In {}, TIMES: {} AVGRT: {:.2}",
                name,
                key,
                window,
                summary.times,
                summary.average,
            );
        } else {
            tracing::info!(
                target: "org.apache.rocketmq.client.stat.ConsumerStatsManager",
                "[{}] [{}] Stats In {}, SUM: {} TPS: {:.2} AVGPT: {:.2}",
                name,
                key,
                window,
                summary.sum,
                summary.tps,
                summary.average,
            );
        }
    }
}

struct SampleWindow {
    interval: Duration,
    capacity: usize,
    samples: VecDeque<Snapshot>,
    sampling_origin: Instant,
    next_sample: Option<Instant>,
}

impl SampleWindow {
    fn minute(sampling_origin: Instant) -> Self {
        Self::new(MINUTE_SAMPLE_INTERVAL, MINUTE_SAMPLE_COUNT, sampling_origin)
    }

    fn hour(sampling_origin: Instant) -> Self {
        Self::new(HOUR_SAMPLE_INTERVAL, HOUR_SAMPLE_COUNT, sampling_origin)
    }

    fn day(sampling_origin: Instant) -> Self {
        Self::new(DAY_SAMPLE_INTERVAL, DAY_SAMPLE_COUNT, sampling_origin)
    }

    fn new(interval: Duration, capacity: usize, sampling_origin: Instant) -> Self {
        Self {
            interval,
            capacity,
            samples: VecDeque::with_capacity(capacity),
            sampling_origin,
            next_sample: None,
        }
    }

    fn schedule_after(&mut self, now: Instant) {
        if self.next_sample.is_some() || self.interval.is_zero() {
            return;
        }
        let elapsed = now.saturating_duration_since(self.sampling_origin);
        let interval_nanos = self.interval.as_nanos();
        let ticks = elapsed.as_nanos() / interval_nanos + 1;
        self.next_sample = Some(
            self.sampling_origin
                .checked_add(duration_from_nanos(interval_nanos.saturating_mul(ticks)))
                .unwrap_or_else(|| now.checked_add(self.interval).unwrap_or(now)),
        );
    }

    fn sample_until(&mut self, now: Instant, value: i64, calls: i64) {
        if self.capacity == 0 || self.interval.is_zero() {
            return;
        }
        let Some(next_sample) = self.next_sample else {
            return;
        };
        if next_sample > now {
            return;
        }

        let interval_nanos = self.interval.as_nanos();
        let due = now.saturating_duration_since(next_sample).as_nanos() / interval_nanos + 1;
        let retained = due.min(self.capacity as u128);
        let skipped = due - retained;
        let first_retained = next_sample
            .checked_add(duration_from_nanos(interval_nanos.saturating_mul(skipped)))
            .unwrap_or(now);

        if self.samples.is_empty() {
            self.push(Snapshot {
                at: next_sample
                    .checked_sub(self.interval)
                    .unwrap_or(next_sample),
                value: 0,
                calls: 0,
            });
        }

        let retained = usize::try_from(retained).unwrap_or(self.capacity);
        let mut sample_at = first_retained;
        for index in 0..retained {
            self.push(Snapshot {
                at: sample_at,
                value,
                calls,
            });
            if index + 1 < retained {
                sample_at = sample_at.checked_add(self.interval).unwrap_or(now);
            }
        }
        let last_sample = next_sample
            .checked_add(duration_from_nanos(
                interval_nanos.saturating_mul(due.saturating_sub(1)),
            ))
            .unwrap_or(now);
        self.next_sample = last_sample.checked_add(self.interval);
    }

    fn push(&mut self, sample: Snapshot) {
        self.samples.push_back(sample);
        while self.samples.len() > self.capacity {
            self.samples.pop_front();
        }
    }

    fn rate(&self) -> f64 {
        let Some((value, _)) = self.delta() else {
            return 0.0;
        };
        let first = self.samples.front().expect("delta requires two samples");
        let last = self.samples.back().expect("delta requires two samples");
        let elapsed = last.at.saturating_duration_since(first.at).as_secs_f64();
        if elapsed == 0.0 {
            0.0
        } else {
            value as f64 / elapsed
        }
    }

    fn average(&self) -> (f64, i64) {
        let Some((value, calls)) = self.delta() else {
            return (0.0, 0);
        };
        if calls == 0 {
            (0.0, 0)
        } else {
            (value as f64 / calls as f64, value)
        }
    }

    fn sum(&self) -> i64 {
        self.delta().map_or(0, |(value, _)| value)
    }

    fn summary(&self) -> MetricSummary {
        let Some((sum, times)) = self.delta() else {
            return MetricSummary::default();
        };
        let first = self.samples.front().expect("delta requires two samples");
        let last = self.samples.back().expect("delta requires two samples");
        let elapsed = last.at.saturating_duration_since(first.at).as_secs_f64();
        MetricSummary {
            sum,
            times,
            tps: if elapsed == 0.0 {
                0.0
            } else {
                sum as f64 / elapsed
            },
            average: if times <= 0 {
                0.0
            } else {
                sum as f64 / times as f64
            },
        }
    }

    fn delta(&self) -> Option<(i64, i64)> {
        let (Some(first), Some(last)) = (self.samples.front(), self.samples.back()) else {
            return None;
        };
        Some((
            last.value.wrapping_sub(first.value),
            last.calls.wrapping_sub(first.calls),
        ))
    }
}

#[derive(Default)]
struct MetricSummary {
    sum: i64,
    times: i64,
    tps: f64,
    average: f64,
}

#[derive(Clone, Copy)]
struct Snapshot {
    at: Instant,
    value: i64,
    calls: i64,
}

fn duration_from_nanos(nanos: u128) -> Duration {
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

fn java_int(value: i64) -> i64 {
    i64::from(value as i32)
}

fn java_stats_key(group: &str, topic: &str) -> String {
    format!("{topic}@{group}")
}

#[cfg(test)]
mod tests {
    use super::{
        ConsumerConsumeStatus, ConsumerStatsManager, MetricSeries, StatsItemSetKind, StatsLogWindow,
    };
    use std::time::{Duration, Instant};

    #[test]
    fn metrics_remain_zero_until_the_first_java_periodic_sample() {
        let origin = Instant::now();
        let mut series = MetricSeries::new(origin);
        series.record(origin + Duration::from_secs(1), 20, 1);
        series.advance(origin + Duration::from_secs(2));

        assert!(series.minute.samples.is_empty());
        assert_eq!(series.minute.rate(), 0.0);
        assert_eq!(series.minute.average(), (0.0, 0));

        series.advance(origin + Duration::from_secs(10));
        assert_eq!(series.minute.rate(), 2.0);
        assert_eq!(series.minute.average(), (20.0, 20));

        series.record(origin + Duration::from_secs(11), 5, 1);
        series.advance(origin + Duration::from_secs(19));
        assert_eq!(series.minute.rate(), 2.0);
        assert_eq!(series.minute.average(), (20.0, 20));

        series.advance(origin + Duration::from_secs(20));
        assert_eq!(series.minute.average(), (12.5, 25));
        assert_eq!(series.minute.rate(), 1.25);

        series.advance(origin + Duration::from_secs(70));
        assert_eq!(series.minute.average(), (5.0, 5));
        assert!((series.minute.rate() - (5.0 / 60.0)).abs() < f64::EPSILON);
        assert_eq!(series.hour.sum(), 0);

        series.advance(origin + Duration::from_secs(80));
        assert_eq!(series.minute.average(), (0.0, 0));
        assert_eq!(series.minute.rate(), 0.0);
        assert_eq!(series.hour.sum(), 0);

        series.advance(origin + Duration::from_secs(600));
        assert_eq!(series.hour.sum(), 25);
    }

    #[test]
    fn daily_stats_window_uses_the_java_hour_sample_cadence() {
        let origin = Instant::now();
        let mut series = MetricSeries::new(origin);
        series.record(origin + Duration::from_secs(1), 12, 3);

        series.advance(origin + Duration::from_secs(60 * 60));

        assert_eq!(series.day.average(), (4.0, 12));
        assert!((series.day.rate() - (12.0 / 3600.0)).abs() < f64::EPSILON);
        assert_eq!(series.day.summary().times, 3);
    }

    #[test]
    fn scheduled_sampling_advances_only_its_java_stats_item_set() {
        let origin = Instant::now();
        let stats = ConsumerStatsManager::with_sampling_origin(origin);
        stats.update_topic_at(
            "workers",
            "orders",
            origin + Duration::from_secs(1),
            |topic, now| {
                for set in StatsItemSetKind::ALL {
                    topic.series_mut(set).record(now, 10, 1);
                }
            },
        );

        stats.sample_at(StatsItemSetKind::PullTps, origin + Duration::from_secs(10));

        let topics = stats.lock_topics();
        for set in StatsItemSetKind::ALL {
            let samples = &topics["orders@workers"].series(set).minute.samples;
            if set == StatsItemSetKind::PullTps {
                assert_eq!(samples.len(), 2);
            } else {
                assert!(samples.is_empty(), "{set:?} was sampled by PullTPS");
            }
        }
    }

    #[test]
    fn each_scheduled_print_emits_only_its_java_stats_item_set() {
        let origin = Instant::now();
        let stats = ConsumerStatsManager::with_sampling_origin(origin);
        stats.update_topic_at(
            "workers",
            "orders",
            origin + Duration::from_secs(1),
            |topic, now| {
                for set in StatsItemSetKind::ALL {
                    topic.series_mut(set).record(now, 10, 1);
                }
            },
        );
        let expected = [
            (StatsItemSetKind::ConsumeOkTps, "CONSUME_OK_TPS", false),
            (StatsItemSetKind::ConsumeRt, "CONSUME_RT", true),
            (
                StatsItemSetKind::ConsumeFailedTps,
                "CONSUME_FAILED_TPS",
                false,
            ),
            (StatsItemSetKind::PullTps, "PULL_TPS", false),
            (StatsItemSetKind::PullRt, "PULL_RT", true),
        ];

        let mut total_events = 0;
        for (set, name, response_time) in expected {
            let events = stats.log_events(set, StatsLogWindow::Minute);
            assert_eq!(events.len(), 1, "{set:?} should emit one metric");
            assert_eq!(events[0].name, name);
            assert_eq!(events[0].key, "orders@workers");
            assert_eq!(events[0].window, "One Minute");
            assert_eq!(events[0].response_time, response_time);
            total_events += events.len();
        }

        assert_eq!(total_events, 5);
    }

    #[test]
    fn consume_status_is_partitioned_and_failed_results_do_not_count_as_success_calls() {
        let origin = Instant::now();
        let stats = ConsumerStatsManager::with_sampling_origin(origin);
        stats.update_topic_at(
            "workers",
            "orders",
            origin + Duration::from_secs(1),
            |topic, now| {
                topic.pull_rt.record(now, 500, 1);
                topic.pull_tps.record(now, 2, 1);
                topic.consume_rt.record(now, 8, 1);
                topic.consume_ok_tps.record(now, 3, 1);
                topic.consume_failed_tps.record(now, 1, 1);
                topic.consume_failed_tps.record(now, 2, 1);
            },
        );

        let before_sample = stats.status_at("workers", "orders", origin + Duration::from_secs(2));
        assert_eq!(before_sample.get_pull_rt(), 0.0);
        assert_eq!(before_sample.get_pull_tps(), 0.0);
        assert_eq!(before_sample.get_consume_rt(), 0.0);

        let minute_target = origin + Duration::from_secs(10);
        let status = stats.status_at("workers", "orders", minute_target);

        assert_eq!(status.get_pull_rt(), 500.0);
        assert_eq!(status.get_consume_rt(), 8.0);
        assert_eq!(status.get_pull_tps(), 0.2);
        assert_eq!(status.get_consume_ok_tps(), 0.3);
        assert_eq!(status.get_consume_failed_tps(), 0.3);
        assert_eq!(status.get_consume_failed_msgs(), 0);
        let hour_target = origin + Duration::from_secs(10 * 60);
        let hour_status = stats.status_at("workers", "orders", hour_target);
        assert_eq!(hour_status.get_consume_failed_msgs(), 3);
        assert_eq!(
            serde_json::to_value(stats.status_at("workers", "other-topic", hour_target)).unwrap(),
            serde_json::to_value(ConsumerConsumeStatus::default()).unwrap()
        );
        assert_eq!(
            serde_json::to_value(stats.status_at("other-group", "orders", hour_target)).unwrap(),
            serde_json::to_value(ConsumerConsumeStatus::default()).unwrap()
        );
    }

    #[test]
    fn public_long_inputs_narrow_like_java_int_arguments() {
        assert_eq!(
            super::java_int(i64::from(i32::MAX) + 1),
            i64::from(i32::MIN)
        );
        assert_eq!(super::java_int(-1), -1);
    }

    #[test]
    fn stats_keys_match_java_topic_group_concatenation() {
        let origin = Instant::now();
        let stats = ConsumerStatsManager::with_sampling_origin(origin);
        stats.update_topic_at(
            "workers@blue",
            "orders",
            origin + Duration::from_secs(1),
            |topic, now| {
                topic.pull_rt.record(now, 3, 1);
            },
        );
        stats.update_topic_at(
            "blue",
            "orders@workers",
            origin + Duration::from_secs(1),
            |topic, now| {
                topic.pull_rt.record(now, 5, 1);
            },
        );

        let first = stats
            .status_at("workers@blue", "orders", origin + Duration::from_secs(10))
            .get_pull_rt();
        let colliding = stats
            .status_at("blue", "orders@workers", origin + Duration::from_secs(10))
            .get_pull_rt();
        assert_eq!(first, 4.0);
        assert_eq!(first, colliding);
        assert_eq!(stats.lock_topics().len(), 1);
    }

    #[test]
    fn sampling_matches_the_pinned_java_consumer_stats_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        let origin = Instant::now();
        let stats = ConsumerStatsManager::with_sampling_origin(origin);
        stats.update_topic_at(
            "workers",
            "orders",
            origin + Duration::from_secs(1),
            |topic, now| {
                topic.pull_tps.record(now, 20, 1);
            },
        );

        let before_sample = stats
            .status_at("workers", "orders", origin + Duration::from_secs(2))
            .get_pull_tps();
        let after_sample = stats
            .status_at("workers", "orders", origin + Duration::from_secs(10))
            .get_pull_tps();
        stats.update_topic_at(
            "workers",
            "orders",
            origin + Duration::from_secs(11),
            |topic, now| {
                topic.pull_tps.record(now, 5, 1);
            },
        );
        let before_next_sample = stats
            .status_at("workers", "orders", origin + Duration::from_secs(19))
            .get_pull_tps();

        assert_eq!(
            before_sample,
            fixture["consumerStatsSamplingBeforeFirstSamplePullTPS"]
                .as_f64()
                .unwrap()
        );
        assert_eq!(
            after_sample,
            fixture["consumerStatsSamplingAfterFirstSamplePullTPS"]
                .as_f64()
                .unwrap()
        );
        assert_eq!(
            before_next_sample,
            fixture["consumerStatsSamplingAfterPendingUpdatePullTPS"]
                .as_f64()
                .unwrap()
        );
    }
}
