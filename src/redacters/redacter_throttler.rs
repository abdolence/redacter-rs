use crate::common_types::DlpRequestLimit;
use std::future::Future;
use std::ops::Add;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

tokio::task_local! {
    static CALL_PACING: CallPacing;
}

/// `--limit-dlp-requests` applied to every call a redacter makes to its provider, whichever
/// file or piece of a file the call is for. One pacing is shared by all the calls of a command,
/// concurrent ones included.
///
/// The pacing is handed to the calls through the task that makes them (see [`Self::scoped`])
/// because the calls are made deep inside the redacters, where no command option is in reach.
#[derive(Clone)]
pub struct CallPacing {
    throttler: Arc<Mutex<RedacterThrottler>>,
    on_delay: Arc<dyn Fn(Duration) + Send + Sync>,
}

impl CallPacing {
    /// A pacing that lets `limit` calls through, and tells `on_delay` how long each call that
    /// has to wait is held.
    pub fn new(
        limit: &DlpRequestLimit,
        on_delay: impl Fn(Duration) + Send + Sync + 'static,
    ) -> Self {
        Self {
            throttler: Arc::new(Mutex::new(limit.to_throttling_counter())),
            on_delay: Arc::new(on_delay),
        }
    }

    /// Runs `work` with `pacing` applied to the calls it makes, or without any when there is
    /// none.
    pub async fn scoped<F: Future>(pacing: &Option<Self>, work: F) -> F::Output {
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
        let delay = {
            let mut throttler = pacing
                .throttler
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *throttler = throttler.update(Instant::now());
            *throttler.delay()
        };
        if !delay.is_zero() {
            (pacing.on_delay)(delay);
            tokio::time::sleep(delay).await;
        }
    }
}

#[derive(Clone, Debug)]
pub struct RedacterThrottler {
    capacity: i64,
    max_capacity: usize,
    last_arrived: Instant,
    last_updated: Instant,
    rate_limit_in_millis: u64,
    delay: Duration,
}

impl RedacterThrottler {
    pub fn new(max_capacity: usize, rate_limit_in_millis: u64) -> Self {
        Self {
            capacity: max_capacity as i64,
            max_capacity,
            last_arrived: Instant::now(),
            last_updated: Instant::now(),
            rate_limit_in_millis,
            delay: Duration::from_millis(0),
        }
    }

    pub fn update(&self, now: Instant) -> Self {
        let time_elapsed_millis = now
            .checked_duration_since(self.last_arrived)
            .unwrap_or_else(|| Duration::from_millis(0))
            .as_millis() as u64;

        let (arrived, new_last_arrived) = {
            if time_elapsed_millis >= self.rate_limit_in_millis {
                let arrived_in_time = time_elapsed_millis / self.rate_limit_in_millis;
                let new_last_updated = self.last_arrived.add(Duration::from_millis(
                    arrived_in_time * self.rate_limit_in_millis,
                ));
                (arrived_in_time as usize, new_last_updated)
            } else {
                (0usize, self.last_arrived)
            }
        };

        let new_available_capacity =
            std::cmp::min(self.capacity + arrived as i64, self.max_capacity as i64);

        if new_available_capacity > 0 {
            Self {
                capacity: new_available_capacity - 1,
                last_arrived: new_last_arrived,
                last_updated: now,
                delay: Duration::from_millis(0),
                ..self.clone()
            }
        } else {
            let updated_capacity = new_available_capacity - 1;

            let base_delay_in_millis = now
                .checked_duration_since(self.last_updated)
                .map_or(0u64, |d| d.as_millis() as u64);

            let delay_penalty = (self.rate_limit_in_millis as f64 * self.capacity.abs() as f64
                / self.max_capacity as f64) as u64;

            let delay_in_millis = self
                .rate_limit_in_millis
                .saturating_sub(base_delay_in_millis);
            let delay_with_penalty =
                Duration::from_millis(delay_in_millis.saturating_add(delay_penalty));

            Self {
                capacity: updated_capacity,
                last_arrived: new_last_arrived,
                last_updated: now,
                delay: delay_with_penalty,
                ..self.clone()
            }
        }
    }

    pub fn delay(&self) -> &Duration {
        &self.delay
    }
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::*;
    use crate::common_types::DlpRequestLimit;
    use crate::redacters::RedacterProviderOptions;
    use console::Term;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn check_decreased() {
        let rate_limit = DlpRequestLimit::new(15, Duration::from_secs(60));
        let rate_limit_in_ms = rate_limit.to_rate_limit_in_ms();
        let rate_limit_capacity = rate_limit.to_rate_limit_capacity();

        let now = Instant::now();
        let counter = RedacterThrottler::new(rate_limit_capacity, rate_limit_in_ms);
        let updated_counter = counter.update(now.add(Duration::from_millis(rate_limit_in_ms - 1)));

        assert_eq!(updated_counter.last_arrived, counter.last_arrived);
        assert_eq!(updated_counter.delay, Duration::from_millis(0));
        assert_eq!(updated_counter.capacity, counter.capacity - 1);
    }

    #[test]
    fn check_max_available() {
        let rate_limit = DlpRequestLimit::new(15, Duration::from_secs(60));
        let rate_limit_in_ms = rate_limit.to_rate_limit_in_ms();
        let rate_limit_capacity = rate_limit.to_rate_limit_capacity();

        let now = Instant::now();
        let counter = RedacterThrottler::new(rate_limit_capacity, rate_limit_in_ms);
        let updated_counter = counter.update(now.add(Duration::from_millis(rate_limit_in_ms + 1)));

        assert_eq!(updated_counter.delay, Duration::from_millis(0));
        assert_eq!(updated_counter.capacity, (counter.max_capacity - 1) as i64);
    }

    #[test]
    fn check_delay() {
        let counter = DlpRequestLimit::new(15, Duration::from_secs(60)).to_throttling_counter();

        let now = Instant::now();

        let updated_counter =
            (0..counter.capacity + 1).fold(counter.clone(), |result, _| result.update(now));

        assert_eq!(
            updated_counter.delay,
            Duration::from_millis(counter.rate_limit_in_millis)
        );
    }

    fn one_call_per_ten_seconds() -> (Option<CallPacing>, Arc<AtomicUsize>) {
        let delayed = Arc::new(AtomicUsize::new(0));
        let counted = delayed.clone();
        let pacing = CallPacing::new(
            &DlpRequestLimit::new(1, Duration::from_secs(10)),
            move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
            },
        );
        (Some(pacing), delayed)
    }

    #[tokio::test(start_paused = true)]
    async fn every_call_waits_for_its_turn() {
        let (pacing, delayed) = one_call_per_ten_seconds();
        let started = tokio::time::Instant::now();

        CallPacing::scoped(&pacing, async {
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

        assert!(
            started.elapsed() >= Duration::from_secs(20),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(delayed.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn calls_made_at_once_share_the_limit() {
        let (pacing, _) = one_call_per_ten_seconds();
        let started = tokio::time::Instant::now();

        CallPacing::scoped(&pacing, async {
            futures::future::join_all((0..3).map(|_| CallPacing::wait_for_turn())).await;
        })
        .await;

        assert!(
            started.elapsed() >= Duration::from_secs(20),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn calls_under_no_pacing_are_not_held() {
        let started = tokio::time::Instant::now();

        CallPacing::scoped(&None, async {
            for _ in 0..3 {
                CallPacing::wait_for_turn().await;
            }
        })
        .await;

        assert_eq!(started.elapsed(), Duration::ZERO);
    }
}
