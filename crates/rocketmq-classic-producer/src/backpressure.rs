use std::{
    collections::VecDeque,
    marker::PhantomData,
    rc::Rc,
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
};

use tokio::{sync::Notify, time::Instant};

use crate::ProducerError;

struct Waiter {
    ticket: u64,
    permits: i64,
    notify: Arc<Notify>,
}

struct State {
    capacity: i64,
    available: i64,
    in_flight: i64,
    next_ticket: u64,
    waiters: VecDeque<Waiter>,
}

/// A fair, dynamically resizable weighted semaphore for Java-compatible
/// async-send backpressure. Resizing keeps the number of in-flight permits,
/// including a negative available count when the new limit is below current
/// usage.
pub(crate) struct AdjustableSemaphore {
    state: Mutex<State>,
    gate: Arc<ReadWriteGate>,
}

pub(crate) struct AdjustablePermit {
    semaphore: Arc<AdjustableSemaphore>,
    permits: i64,
    gate: Arc<ReadWriteGate>,
}

struct GateState {
    readers: usize,
    waiting_writers: usize,
    writer_active: bool,
}

pub(crate) struct ReadWriteGate {
    state: Mutex<GateState>,
    changed: Condvar,
}

/// Read lock used by Java-compatible public backpressure lock entrypoints.
/// Dropping this guard releases the lock.
#[must_use]
pub struct BackpressureReadGuard {
    _lease: ReadLockLease,
    _not_send_or_sync: PhantomData<Rc<()>>,
}

struct ReadLockLease {
    gate: Arc<ReadWriteGate>,
}

struct BackpressureWriteGuard<'a> {
    gate: &'a ReadWriteGate,
}

struct WaiterTicket {
    semaphore: Arc<AdjustableSemaphore>,
    ticket: u64,
    registered: bool,
}

impl AdjustableSemaphore {
    pub(crate) fn new(capacity: i32) -> Self {
        let capacity = i64::from(capacity);
        Self {
            state: Mutex::new(State {
                capacity,
                available: capacity,
                in_flight: 0,
                next_ticket: 0,
                waiters: VecDeque::new(),
            }),
            gate: Arc::new(ReadWriteGate::new()),
        }
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> i32 {
        self.lock_state().capacity as i32
    }

    pub(crate) fn available_permits(&self) -> i64 {
        self.lock_state().available
    }

    #[cfg(test)]
    pub(crate) fn resize(&self, capacity: i32) {
        self.resize_and(capacity, || {});
    }

    pub(crate) fn resize_and(&self, capacity: i32, update: impl FnOnce()) {
        let _writer = self.gate.write_lock();
        let mut state = self.lock_state();
        state.capacity = i64::from(capacity);
        state.available = state.capacity - state.in_flight;
        update();
        Self::wake_front(&state);
    }

    /// Replaces the Java implementation's raw semaphore count while retaining
    /// the logical capacity used by the facade setting. Java computes the
    /// in-flight delta as `capacity - availablePermits`, so preserve that
    /// accounting relation for later permit releases and facade resizes.
    pub(crate) fn reset_available_permits(&self, available: i32) {
        let _writer = self.gate.write_lock();
        let mut state = self.lock_state();
        state.available = i64::from(available);
        state.in_flight = state.capacity - state.available;
        Self::wake_front(&state);
    }

    pub(crate) async fn acquire(
        self: &Arc<Self>,
        permits: usize,
        deadline: Instant,
    ) -> Result<AdjustablePermit, ProducerError> {
        let permits = i64::try_from(permits).map_err(|_| ProducerError::Timeout)?;
        if permits <= 0 {
            return Err(ProducerError::Timeout);
        }

        // Java holds the producer's read lock across the blocking semaphore
        // acquisition. Keep the equivalent lease while this async wait is
        // pending so a runtime resize cannot change the permit limit midway.
        let _reader = self.gate.read_lock_lease();
        let ticket = {
            let mut state = self.lock_state();
            let ticket = state.next_ticket;
            state.next_ticket = state.next_ticket.wrapping_add(1);
            ticket
        };
        let notify = Arc::new(Notify::new());
        let mut ticket_guard = WaiterTicket {
            semaphore: self.clone(),
            ticket,
            registered: false,
        };

        loop {
            if Instant::now() >= deadline {
                return Err(ProducerError::Timeout);
            }
            let notified = notify.notified();
            let acquired = {
                let mut state = self.lock_state();
                let is_front = state
                    .waiters
                    .front()
                    .is_some_and(|waiter| waiter.ticket == ticket);
                let can_acquire = if state.waiters.is_empty() {
                    state.available >= permits
                } else {
                    is_front && state.available >= permits
                };

                if can_acquire {
                    if is_front {
                        state.waiters.pop_front();
                    }
                    state.available -= permits;
                    state.in_flight += permits;
                    ticket_guard.registered = false;
                    Self::wake_front(&state);
                    true
                } else {
                    if !ticket_guard.registered {
                        state.waiters.push_back(Waiter {
                            ticket,
                            permits,
                            notify: notify.clone(),
                        });
                        ticket_guard.registered = true;
                    }
                    false
                }
            };

            if acquired {
                return Ok(AdjustablePermit {
                    semaphore: self.clone(),
                    permits,
                    gate: self.gate.clone(),
                });
            }

            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(ProducerError::Timeout);
            }
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn read_lock(&self) -> BackpressureReadGuard {
        self.gate.read_lock()
    }

    fn wake_front(state: &State) {
        if let Some(waiter) = state.waiters.front() {
            if state.available >= waiter.permits {
                waiter.notify.notify_one();
            }
        }
    }

    fn remove_waiter(&self, ticket: u64) {
        let mut state = self.lock_state();
        if let Some(index) = state
            .waiters
            .iter()
            .position(|waiter| waiter.ticket == ticket)
        {
            state.waiters.remove(index);
            Self::wake_front(&state);
        }
    }

    fn release(&self, permits: i64) {
        let mut state = self.lock_state();
        state.in_flight -= permits;
        state.available += permits;
        Self::wake_front(&state);
    }
}

impl Drop for AdjustablePermit {
    fn drop(&mut self) {
        let _reader = self.gate.read_lock_lease();
        self.semaphore.release(self.permits);
    }
}

impl ReadWriteGate {
    fn new() -> Self {
        Self {
            state: Mutex::new(GateState {
                readers: 0,
                waiting_writers: 0,
                writer_active: false,
            }),
            changed: Condvar::new(),
        }
    }

    fn read_lock(self: &Arc<Self>) -> BackpressureReadGuard {
        BackpressureReadGuard {
            _lease: self.read_lock_lease(),
            _not_send_or_sync: PhantomData,
        }
    }

    fn read_lock_lease(self: &Arc<Self>) -> ReadLockLease {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while state.writer_active || state.waiting_writers > 0 {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        state.readers += 1;
        drop(state);
        ReadLockLease { gate: self.clone() }
    }

    fn write_lock(&self) -> BackpressureWriteGuard<'_> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.waiting_writers += 1;
        while state.writer_active || state.readers > 0 {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        state.waiting_writers -= 1;
        state.writer_active = true;
        drop(state);
        BackpressureWriteGuard { gate: self }
    }
}

impl Drop for ReadLockLease {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.readers -= 1;
        self.gate.changed.notify_all();
    }
}

impl Drop for BackpressureWriteGuard<'_> {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.writer_active = false;
        self.gate.changed.notify_all();
    }
}

impl Drop for WaiterTicket {
    fn drop(&mut self) {
        if self.registered {
            self.semaphore.remove_waiter(self.ticket);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc, time::Duration};

    #[tokio::test(start_paused = true)]
    async fn resize_preserves_in_flight_permits_and_fair_waiting() {
        let semaphore = Arc::new(AdjustableSemaphore::new(3));
        let first = semaphore
            .acquire(2, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(semaphore.available_permits(), 1);

        semaphore.resize(1);
        assert_eq!(semaphore.available_permits(), -1);
        assert_eq!(semaphore.capacity(), 1);
        assert!(matches!(
            semaphore
                .acquire(1, Instant::now() + Duration::from_millis(1))
                .await,
            Err(ProducerError::Timeout)
        ));

        drop(first);
        assert_eq!(semaphore.available_permits(), 1);
        let permit = semaphore
            .acquire(1, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(semaphore.available_permits(), 0);
        drop(permit);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_waiter_does_not_block_following_acquisitions() {
        let semaphore = Arc::new(AdjustableSemaphore::new(1));
        let held = semaphore
            .acquire(1, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();

        let waiting = {
            let semaphore = semaphore.clone();
            tokio::spawn(async move {
                semaphore
                    .acquire(1, Instant::now() + Duration::from_secs(10))
                    .await
            })
        };
        tokio::task::yield_now().await;
        waiting.abort();
        let _ = waiting.await;
        drop(held);

        assert!(
            semaphore
                .acquire(1, Instant::now() + Duration::from_secs(1))
                .await
                .is_ok()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_resize_waits_for_a_pending_acquisition_like_java() {
        let semaphore = Arc::new(AdjustableSemaphore::new(1));
        let held = semaphore
            .acquire(1, Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();
        let waiting = {
            let semaphore = semaphore.clone();
            tokio::spawn(async move {
                semaphore
                    .acquire(1, Instant::now() + Duration::from_secs(10))
                    .await
            })
        };
        tokio::task::yield_now().await;

        let (started_tx, started_rx) = mpsc::channel();
        let (resized_tx, resized_rx) = mpsc::channel();
        let setter = {
            let semaphore = semaphore.clone();
            std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                semaphore.resize_and(2, || {});
                resized_tx.send(()).unwrap();
            })
        };
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let writer_wait_deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let waiting_writers = semaphore
                .gate
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .waiting_writers;
            if waiting_writers > 0 {
                break;
            }
            assert!(std::time::Instant::now() < writer_wait_deadline);
            std::thread::yield_now();
        }
        assert!(resized_rx.recv_timeout(Duration::from_millis(10)).is_err());

        assert!(matches!(
            waiting.await.unwrap(),
            Err(ProducerError::Timeout)
        ));
        resized_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        setter.join().unwrap();
        assert_eq!(semaphore.capacity(), 2);
        drop(held);
        assert_eq!(semaphore.available_permits(), 2);
    }

    #[test]
    fn runtime_resize_waits_for_a_public_read_lock_guard() {
        let semaphore = Arc::new(AdjustableSemaphore::new(10));
        let guard = semaphore.read_lock();
        let (started_tx, started_rx) = mpsc::channel();
        let (resized_tx, resized_rx) = mpsc::channel();
        let setter = {
            let semaphore = semaphore.clone();
            std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                semaphore.resize_and(12, || {});
                resized_tx.send(()).unwrap();
            })
        };

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(resized_rx.recv_timeout(Duration::from_millis(10)).is_err());
        drop(guard);
        resized_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        setter.join().unwrap();
        assert_eq!(semaphore.capacity(), 12);
    }
}
