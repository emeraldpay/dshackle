// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A set that forgets: members expire after a time-to-live, and the oldest
//! ones make room once the set is full. Ports the legacy `ExpiringSet`.

use std::collections::{HashSet, VecDeque};
use std::hash::Hash;
use std::time::Duration;
use tokio::time::Instant;

/// Remembers recently seen values, bounded both in time and in size.
pub(super) struct ExpiringSet<T> {
    ttl: Duration,
    limit: usize,
    members: HashSet<T>,
    /// When each member joined, oldest first.
    journal: VecDeque<(Instant, T)>,
}

impl<T: Hash + Eq + Clone> ExpiringSet<T> {
    /// A set whose members live for `ttl`, holding at most `limit` of them.
    pub(super) fn new(ttl: Duration, limit: usize) -> Self {
        Self {
            ttl,
            limit,
            members: HashSet::new(),
            journal: VecDeque::new(),
        }
    }

    /// Adds `value`, returning `false` when it is already remembered.
    pub(super) fn insert(&mut self, value: T) -> bool {
        self.forget_expired();
        if !self.members.insert(value.clone()) {
            return false;
        }
        self.journal.push_back((Instant::now(), value));
        if self.journal.len() > self.limit
            && let Some((_, oldest)) = self.journal.pop_front()
        {
            self.members.remove(&oldest);
        }
        true
    }

    fn forget_expired(&mut self) {
        while let Some((since, _)) = self.journal.front() {
            if since.elapsed() < self.ttl {
                break;
            }
            if let Some((_, expired)) = self.journal.pop_front() {
                self.members.remove(&expired);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn remembers_a_value_until_it_expires() {
        let mut set = ExpiringSet::new(Duration::from_secs(30), 100);
        assert!(set.insert("a"));
        assert!(!set.insert("a"));

        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(!set.insert("a"));

        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(set.insert("a"));
    }

    #[tokio::test(start_paused = true)]
    async fn expiry_counts_from_the_first_sighting() {
        let mut set = ExpiringSet::new(Duration::from_secs(30), 100);
        assert!(set.insert("a"));
        tokio::time::advance(Duration::from_secs(20)).await;
        // A repeated sighting doesn't prolong the memory of the value.
        assert!(!set.insert("a"));
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(set.insert("a"));
    }

    #[tokio::test(start_paused = true)]
    async fn forgets_the_oldest_when_full() {
        let mut set = ExpiringSet::new(Duration::from_secs(30), 2);
        assert!(set.insert("a"));
        assert!(set.insert("b"));
        assert!(set.insert("c"));

        assert!(!set.insert("c"));
        assert!(!set.insert("b"));
        assert!(set.insert("a"));
    }
}
