use std::sync::Weak;

use super::ClientInner;

pub(super) struct PendingGuard {
    inner: Weak<ClientInner>,
    opaque: i32,
    generation: u64,
    armed: bool,
}

impl PendingGuard {
    pub(super) fn new(inner: Weak<ClientInner>, opaque: i32, generation: u64) -> Self {
        Self {
            inner,
            opaque,
            generation,
            armed: true,
        }
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if self.armed {
            if let Some(inner) = self.inner.upgrade() {
                inner.cancel_pending(self.opaque, self.generation);
            }
        }
    }
}
