use crate::common_types::DlpRequestLimit;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

tokio::task_local! {
    static CALL_PACING: CallPacing;
}

/// `--limit-dlp-requests` applied to every call a redacter makes to its provider, whichever
/// file or piece of a file the call is for. One pacing is shared by all the calls of a command,
/// concurrent ones included.
///
/// The pacing reaches the calls through the task that makes them, because they are made deep
/// inside the redacters, where no command option is in reach. A command must therefore run its
/// redaction or detection inside [`Self::scoped`], in the same task: calls made outside it, or
/// in a task spawned from it, are not paced, and nothing reports that.
#[derive(Clone)]
pub struct CallPacing {
    slots: Arc<Mutex<Slots>>,
}

/// The times calls may start at: one every `interval`, with up to `limit - 1` calls ahead of
/// that rate after a quiet spell. A call reserves its slot when it asks, so calls that ask at
/// once are spread out rather than all measured against the same clock.
struct Slots {
    interval: Duration,
    burst: Duration,
    next: Instant,
}

impl Slots {
    fn reserve(&mut self, now: Instant) -> Instant {
        let slot = self
            .next
            .checked_sub(self.burst)
            .map_or(now, |earliest| earliest.max(now));
        self.next = self.next.max(now) + self.interval;
        slot
    }
}

impl CallPacing {
    pub fn new(limit: &DlpRequestLimit) -> Self {
        let interval = limit.per.div_f64(limit.value as f64);
        Self {
            slots: Arc::new(Mutex::new(Slots {
                interval,
                burst: limit.per - interval,
                next: Instant::now(),
            })),
        }
    }

    /// Runs `work` with `pacing` applied to the calls it makes, or without any when there is
    /// none.
    pub async fn scoped<F: Future>(pacing: Option<&Self>, work: F) -> F::Output {
        match pacing {
            Some(pacing) => CALL_PACING.scope(pacing.clone(), work).await,
            None => work.await,
        }
    }

    /// Returns when the call about to be made is within the limit of the pacing it runs under,
    /// at once when it runs under none. Every call asks, and a call that is made again after a
    /// failure asks again.
    pub async fn wait_for_turn() {
        let Ok(pacing) = CALL_PACING.try_with(Clone::clone) else {
            return;
        };
        let now = Instant::now();
        let slot = pacing
            .slots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .reserve(now);
        if slot > now {
            tracing::debug!(delay = ?slot - now, "a call waits for the request limit");
            tokio::time::sleep_until(slot).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn every_call_waits_for_its_turn() {
        let pacing = Some(CallPacing::new(&DlpRequestLimit::new(
            1,
            Duration::from_secs(10),
        )));
        let started = Instant::now();

        CallPacing::scoped(pacing.as_ref(), async {
            CallPacing::wait_for_turn().await;
            assert_eq!(
                started.elapsed(),
                Duration::ZERO,
                "the first call is not held"
            );
            CallPacing::wait_for_turn().await;
            CallPacing::wait_for_turn().await;
        })
        .await;

        assert_eq!(started.elapsed(), Duration::from_secs(20));
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_callers_share_the_limit_over_several_periods() {
        let pacing = Some(CallPacing::new(&DlpRequestLimit::new(
            4,
            Duration::from_secs(10),
        )));
        let started = Instant::now();
        let span = Duration::from_secs(30);

        let starts = CallPacing::scoped(pacing.as_ref(), async {
            let callers = (0..4).map(|_| async {
                let mut starts = Vec::new();
                while started.elapsed() < span {
                    CallPacing::wait_for_turn().await;
                    starts.push(started.elapsed());
                }
                starts
            });
            futures::future::join_all(callers).await
        })
        .await;

        let calls_in_span = starts.iter().flatten().filter(|at| **at < span).count();
        assert_eq!(calls_in_span, 4 + 11);
    }
}
