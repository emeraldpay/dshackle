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

//! Per-block fee cache shared by the chain fee estimators.
//!
//! An estimate samples one transaction per block over a window of up to a few
//! hundred blocks, so without a cache every call re-fetches every full block.
//! The sampled blocks are historical and their contents are immutable, so a
//! computed per-block fee stays valid and can be reused across calls. Mirrors
//! the legacy `AbstractChainFees` cache.

use super::FeeMode;
use moka::sync::Cache;
use std::time::Duration;

/// How long a computed per-block fee stays cached, from write. Matches the
/// legacy `expireAfterWrite(Duration.ofMinutes(60))`: long enough to serve
/// repeated estimates cheaply, short enough to bound both staleness and growth.
const TTL: Duration = Duration::from_secs(60 * 60);

/// A `(height, mode)` → fee cache. The mode is part of the key because each mode
/// samples a different transaction position, so one block yields a different fee
/// per mode. Generic over the fee value so both the Ethereum and Bitcoin
/// estimators can share it.
pub struct FeeCache<F> {
    entries: Cache<(u64, FeeMode), F>,
}

impl<F: Clone + Send + Sync + 'static> FeeCache<F> {
    pub fn new() -> Self {
        Self {
            entries: Cache::builder().time_to_live(TTL).build(),
        }
    }

    /// The cached fee for this block and mode, if one is still live.
    pub fn get(&self, height: u64, mode: FeeMode) -> Option<F> {
        self.entries.get(&(height, mode))
    }

    /// Record the fee computed for this block and mode.
    pub fn put(&self, height: u64, mode: FeeMode, fee: F) {
        self.entries.insert((height, mode), fee);
    }
}
