use super::cache_types::Ticket;
use super::writeback_cache::{
    CachedCommittedData, LatestObjectCacheEntry, ObjectEntry, PointCacheItem, UncommittedData,
};
use super::WritebackCache;
use super::{
    cache_types::{CacheResult, CachedVersionMap, MonotonicCache},
    object_locks::ObjectLocks,
    Batch, ExecutionCacheCommit, ExecutionCacheMetrics, ObjectCacheRead, TransactionCacheRead,
};
use crate::authority::authority_per_epoch_store::AuthorityPerEpochStore;
use crate::authority::authority_store::{
    ExecutionLockWriteGuard, LockDetailsDeprecated, ObjectLockStatus, SuiLockResult,
};
use crate::authority::backpressure::BackpressureManager;
use crate::authority::readonly_authority_store::ReadonlyAuthorityStore;
use crate::execution_cache::writeback_cache::assert_empty;
use crate::fallback_fetch::{do_fallback_lookup, do_fallback_lookup_fallible};
use crate::transaction_outputs::TransactionOutputs;
use crate::{check_cache_entry_by_latest, check_cache_entry_by_version};
use dashmap::mapref::entry::Entry as DashMapEntry;
use dashmap::DashMap;
use futures::{future::BoxFuture, FutureExt};
use moka::sync::SegmentedCache as MokaCache;
use mysten_common::random_util::randomize_cache_capacity_in_tests;
use mysten_common::sync::notify_read::NotifyRead;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;
use sui_config::ExecutionCacheConfig;
use sui_types::base_types::{EpochId, FullObjectID, ObjectID, ObjectRef, SequenceNumber};
use sui_types::bridge::{get_bridge, Bridge};
use sui_types::digests::{ObjectDigest, TransactionDigest, TransactionEffectsDigest};
use sui_types::effects::{TransactionEffects, TransactionEvents};
use sui_types::error::{SuiError, SuiResult, UserInputError};
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use sui_types::object::Object;
use sui_types::storage::{
    FullObjectKey, InputKey, MarkerValue, ObjectKey, ObjectOrTombstone, ObjectStore, PackageObject,
};
use sui_types::sui_system_state::{get_sui_system_state, SuiSystemState};
use sui_types::transaction::VerifiedTransaction;
use tap::TapOptional;
use tracing::{info, instrument, trace, warn};

pub struct ReadonlyWritebackCache {
    dirty: UncommittedData,
    cached: CachedCommittedData,

    // We separately cache the latest version of each object. Although this seems
    // redundant, it is the only way to support populating the cache after a read.
    // We cannot simply insert objects that we read off the disk into `object_cache`,
    // since that may violate the no-missing-versions property.
    // `object_by_id_cache` is also written to on writes so that it is always coherent.
    // Hence it contains both committed and dirty object data.
    object_by_id_cache: MonotonicCache<ObjectID, LatestObjectCacheEntry>,

    // The packages cache is treated separately from objects, because they are immutable and can be
    // used by any number of transactions. Additionally, many operations require loading large
    // numbers of packages (due to dependencies), so we want to try to keep all packages in memory.
    //
    // Also, this cache can contain packages that are dirty or committed, so it does not live in
    // UncachedData or CachedCommittedData. The cache is populated in two ways:
    // - when packages are written (in which case they will also be present in the dirty set)
    // - after a cache miss. Because package IDs are unique (only one version exists for each ID)
    //   we do not need to worry about the contiguous version property.
    // - note that we removed any unfinalized packages from the cache during revert_state_update().
    packages: MokaCache<ObjectID, PackageObject>,

    object_locks: ObjectLocks,

    executed_effects_digests_notify_read: NotifyRead<TransactionDigest, TransactionEffectsDigest>,
    object_notify_read: NotifyRead<InputKey, ()>,
    fastpath_transaction_outputs_notify_read:
        NotifyRead<TransactionDigest, Arc<TransactionOutputs>>,

    store: Arc<ReadonlyAuthorityStore>,
    backpressure_threshold: u64,
    backpressure_manager: Arc<BackpressureManager>,
    metrics: Arc<ExecutionCacheMetrics>,
}

impl ReadonlyWritebackCache {
    pub fn new(
        config: &ExecutionCacheConfig,
        store: Arc<ReadonlyAuthorityStore>,
        metrics: Arc<ExecutionCacheMetrics>,
        backpressure_manager: Arc<BackpressureManager>,
    ) -> Self {
        let packages = MokaCache::builder(8)
            .max_capacity(randomize_cache_capacity_in_tests(
                config.package_cache_size(),
            ))
            .build();
        Self {
            dirty: UncommittedData::new(config),
            cached: CachedCommittedData::new(config),
            object_by_id_cache: MonotonicCache::new(randomize_cache_capacity_in_tests(
                config.object_by_id_cache_size(),
            )),
            packages,
            object_locks: ObjectLocks::new(),
            executed_effects_digests_notify_read: NotifyRead::new(),
            object_notify_read: NotifyRead::new(),
            fastpath_transaction_outputs_notify_read: NotifyRead::new(),
            store,
            backpressure_manager,
            backpressure_threshold: config.backpressure_threshold(),
            metrics,
        }
    }

    // lock both the dirty and committed sides of the cache, and then pass the entries to
    // the callback. Written with the `with` pattern because any other way of doing this
    // creates lifetime hell.
    fn with_locked_cache_entries<K, V, R>(
        dirty_map: &DashMap<K, CachedVersionMap<V>>,
        cached_map: &MokaCache<K, Arc<Mutex<CachedVersionMap<V>>>>,
        key: &K,
        cb: impl FnOnce(Option<&CachedVersionMap<V>>, Option<&CachedVersionMap<V>>) -> R,
    ) -> R
    where
        K: Copy + Eq + Hash + Send + Sync + 'static,
        V: Send + Sync + 'static,
    {
        let dirty_entry = dirty_map.entry(*key);
        let dirty_entry = match &dirty_entry {
            DashMapEntry::Occupied(occupied) => Some(occupied.get()),
            DashMapEntry::Vacant(_) => None,
        };

        let cached_entry = cached_map.get(key);
        let cached_lock = cached_entry.as_ref().map(|entry| entry.lock());
        let cached_entry = cached_lock.as_deref();

        cb(dirty_entry, cached_entry)
    }

    // Attempt to get an object from the cache. The DB is not consulted.
    // Can return Hit, Miss, or NegativeHit (if the object is known to not exist).
    fn get_object_entry_by_key_cache_only(
        &self,
        object_id: &ObjectID,
        version: SequenceNumber,
    ) -> CacheResult<ObjectEntry> {
        Self::with_locked_cache_entries(
            &self.dirty.objects,
            &self.cached.object_cache,
            object_id,
            |dirty_entry, cached_entry| {
                check_cache_entry_by_version!(
                    self,
                    "object_by_version",
                    "uncommitted",
                    dirty_entry,
                    version
                );
                check_cache_entry_by_version!(
                    self,
                    "object_by_version",
                    "committed",
                    cached_entry,
                    version
                );
                CacheResult::Miss
            },
        )
    }

    fn get_object_by_key_cache_only(
        &self,
        object_id: &ObjectID,
        version: SequenceNumber,
    ) -> CacheResult<Object> {
        match self.get_object_entry_by_key_cache_only(object_id, version) {
            CacheResult::Hit(entry) => match entry {
                ObjectEntry::Object(object) => CacheResult::Hit(object),
                ObjectEntry::Deleted | ObjectEntry::Wrapped => CacheResult::NegativeHit,
            },
            CacheResult::Miss => CacheResult::Miss,
            CacheResult::NegativeHit => CacheResult::NegativeHit,
        }
    }

    fn get_object_entry_by_id_cache_only(
        &self,
        request_type: &'static str,
        object_id: &ObjectID,
    ) -> CacheResult<(SequenceNumber, ObjectEntry)> {
        self.metrics
            .record_cache_request(request_type, "object_by_id");
        let entry = self.object_by_id_cache.get(object_id);

        if cfg!(debug_assertions) {
            if let Some(entry) = &entry {
                // check that cache is coherent
                let highest: Option<ObjectEntry> = self
                    .dirty
                    .objects
                    .get(object_id)
                    .and_then(|entry| entry.get_highest().map(|(_, o)| o.clone()))
                    .or_else(|| {
                        let obj: Option<ObjectEntry> = self
                            .store
                            .get_latest_object_or_tombstone(*object_id)
                            .unwrap()
                            .map(|(_, o)| o.into());
                        obj
                    });

                let cache_entry = match &*entry.lock() {
                    LatestObjectCacheEntry::Object(_, entry) => Some(entry.clone()),
                    LatestObjectCacheEntry::NonExistent => None,
                };

                // If the cache entry is a tombstone, the db entry may be missing if it was pruned.
                let tombstone_possibly_pruned = highest.is_none()
                    && cache_entry
                        .as_ref()
                        .map(|e| e.is_tombstone())
                        .unwrap_or(false);

                if highest != cache_entry && !tombstone_possibly_pruned {
                    tracing::error!(
                        ?highest,
                        ?cache_entry,
                        ?tombstone_possibly_pruned,
                        "object_by_id cache is incoherent for {:?}",
                        object_id
                    );
                    panic!("object_by_id cache is incoherent for {:?}", object_id);
                }
            }
        }

        if let Some(entry) = entry {
            let entry = entry.lock();
            match &*entry {
                LatestObjectCacheEntry::Object(latest_version, latest_object) => {
                    self.metrics.record_cache_hit(request_type, "object_by_id");
                    return CacheResult::Hit((*latest_version, latest_object.clone()));
                }
                LatestObjectCacheEntry::NonExistent => {
                    self.metrics
                        .record_cache_negative_hit(request_type, "object_by_id");
                    return CacheResult::NegativeHit;
                }
            }
        } else {
            self.metrics.record_cache_miss(request_type, "object_by_id");
        }

        Self::with_locked_cache_entries(
            &self.dirty.objects,
            &self.cached.object_cache,
            object_id,
            |dirty_entry, cached_entry| {
                check_cache_entry_by_latest!(self, request_type, "uncommitted", dirty_entry);
                check_cache_entry_by_latest!(self, request_type, "committed", cached_entry);
                CacheResult::Miss
            },
        )
    }

    fn get_object_by_id_cache_only(
        &self,
        request_type: &'static str,
        object_id: &ObjectID,
    ) -> CacheResult<(SequenceNumber, Object)> {
        match self.get_object_entry_by_id_cache_only(request_type, object_id) {
            CacheResult::Hit((version, entry)) => match entry {
                ObjectEntry::Object(object) => CacheResult::Hit((version, object)),
                ObjectEntry::Deleted | ObjectEntry::Wrapped => CacheResult::NegativeHit,
            },
            CacheResult::NegativeHit => CacheResult::NegativeHit,
            CacheResult::Miss => CacheResult::Miss,
        }
    }

    fn get_marker_value_cache_only(
        &self,
        object_key: FullObjectKey,
        epoch_id: EpochId,
    ) -> CacheResult<MarkerValue> {
        Self::with_locked_cache_entries(
            &self.dirty.markers,
            &self.cached.marker_cache,
            &(epoch_id, object_key.id()),
            |dirty_entry, cached_entry| {
                check_cache_entry_by_version!(
                    self,
                    "marker_by_version",
                    "uncommitted",
                    dirty_entry,
                    object_key.version()
                );
                check_cache_entry_by_version!(
                    self,
                    "marker_by_version",
                    "committed",
                    cached_entry,
                    object_key.version()
                );
                CacheResult::Miss
            },
        )
    }

    fn get_latest_marker_value_cache_only(
        &self,
        object_id: FullObjectID,
        epoch_id: EpochId,
    ) -> CacheResult<(SequenceNumber, MarkerValue)> {
        Self::with_locked_cache_entries(
            &self.dirty.markers,
            &self.cached.marker_cache,
            &(epoch_id, object_id),
            |dirty_entry, cached_entry| {
                check_cache_entry_by_latest!(self, "marker_latest", "uncommitted", dirty_entry);
                check_cache_entry_by_latest!(self, "marker_latest", "committed", cached_entry);
                CacheResult::Miss
            },
        )
    }

    fn get_object_impl(&self, request_type: &'static str, id: &ObjectID) -> Option<Object> {
        let ticket = self.object_by_id_cache.get_ticket_for_read(id);
        match self.get_object_entry_by_id_cache_only(request_type, id) {
            CacheResult::Hit((_, entry)) => match entry {
                ObjectEntry::Object(object) => Some(object),
                ObjectEntry::Deleted | ObjectEntry::Wrapped => None,
            },
            CacheResult::NegativeHit => None,
            CacheResult::Miss => {
                let obj = self
                    .store
                    .get_latest_object_or_tombstone(*id)
                    .expect("db error");
                match obj {
                    Some((key, obj)) => {
                        self.cache_latest_object_by_id(
                            id,
                            LatestObjectCacheEntry::Object(key.1, obj.clone().into()),
                            ticket,
                        );
                        match obj {
                            ObjectOrTombstone::Object(object) => Some(object),
                            ObjectOrTombstone::Tombstone(_) => None,
                        }
                    }
                    None => {
                        self.cache_object_not_found(id, ticket);
                        None
                    }
                }
            }
        }
    }

    fn record_db_get(&self, request_type: &'static str) -> &ReadonlyAuthorityStore {
        self.metrics.record_cache_request(request_type, "db");
        &self.store
    }

    fn record_db_multi_get(
        &self,
        request_type: &'static str,
        count: usize,
    ) -> &ReadonlyAuthorityStore {
        self.metrics
            .record_cache_multi_request(request_type, "db", count);
        &self.store
    }

    fn build_db_batch(&self, epoch: EpochId, digests: &[TransactionDigest]) -> Batch {
        let _metrics_guard = mysten_metrics::monitored_scope("WritebackCache::build_db_batch");
        let mut all_outputs = Vec::with_capacity(digests.len());
        for tx in digests {
            let Some(outputs) = self
                .dirty
                .pending_transaction_writes
                .get(tx)
                .map(|o| o.clone())
            else {
                // This can happen in the following rare case:
                // All transactions in the checkpoint are committed to the db (by commit_transaction_outputs,
                // called in CheckpointExecutor::process_executed_transactions), but the process crashes before
                // the checkpoint water mark is bumped. We will then re-commit the checkpoint at startup,
                // despite that all transactions are already executed.
                warn!("Attempt to commit unknown transaction {:?}", tx);
                continue;
            };
            all_outputs.push(outputs);
        }

        let batch = self
            .store
            .build_db_batch(epoch, &all_outputs)
            .expect("db error");
        (all_outputs, batch)
    }

    fn approximate_pending_transaction_count(&self) -> u64 {
        let num_commits = self
            .dirty
            .total_transaction_commits
            .load(std::sync::atomic::Ordering::Relaxed);

        self.dirty
            .total_transaction_inserts
            .load(std::sync::atomic::Ordering::Relaxed)
            .saturating_sub(num_commits)
    }

    fn set_backpressure(&self, pending_count: u64) {
        let backpressure = pending_count > self.backpressure_threshold;
        let backpressure_changed = self.backpressure_manager.set_backpressure(backpressure);
        if backpressure_changed {
            self.metrics.backpressure_toggles.inc();
        }
        self.metrics
            .backpressure_status
            .set(if backpressure { 1 } else { 0 });
    }

    fn flush_transactions_from_dirty_to_cached(
        &self,
        epoch: EpochId,
        tx_digest: TransactionDigest,
        outputs: &TransactionOutputs,
    ) {
        // Now, remove each piece of committed data from the dirty state and insert it into the cache.
        // TODO: outputs should have a strong count of 1 so we should be able to move out of it
        let TransactionOutputs {
            transaction,
            effects,
            markers,
            written,
            deleted,
            wrapped,
            events,
            ..
        } = outputs;

        let effects_digest = effects.digest();

        // Update cache before removing from self.dirty to avoid
        // unnecessary cache misses
        self.cached
            .transactions
            .insert(
                &tx_digest,
                PointCacheItem::Some(transaction.clone()),
                Ticket::Write,
            )
            .ok();
        self.cached
            .transaction_effects
            .insert(
                &effects_digest,
                PointCacheItem::Some(effects.clone().into()),
                Ticket::Write,
            )
            .ok();
        self.cached
            .executed_effects_digests
            .insert(
                &tx_digest,
                PointCacheItem::Some(effects_digest),
                Ticket::Write,
            )
            .ok();
        self.cached
            .transaction_events
            .insert(
                &tx_digest,
                PointCacheItem::Some(events.clone().into()),
                Ticket::Write,
            )
            .ok();

        self.dirty
            .transaction_effects
            .remove(&effects_digest)
            .expect("effects must exist");

        self.dirty
            .transaction_events
            .remove(&tx_digest)
            .expect("events must exist");

        self.dirty
            .executed_effects_digests
            .remove(&tx_digest)
            .expect("executed effects must exist");

        // Move dirty markers to cache
        for (object_key, marker_value) in markers.iter() {
            WritebackCache::move_version_from_dirty_to_cache(
                &self.dirty.markers,
                &self.cached.marker_cache,
                (epoch, object_key.id()),
                object_key.version(),
                marker_value,
            );
        }

        for (object_id, object) in written.iter() {
            WritebackCache::move_version_from_dirty_to_cache(
                &self.dirty.objects,
                &self.cached.object_cache,
                *object_id,
                object.version(),
                &ObjectEntry::Object(object.clone()),
            );
        }

        for ObjectKey(object_id, version) in deleted.iter() {
            WritebackCache::move_version_from_dirty_to_cache(
                &self.dirty.objects,
                &self.cached.object_cache,
                *object_id,
                *version,
                &ObjectEntry::Deleted,
            );
        }

        for ObjectKey(object_id, version) in wrapped.iter() {
            WritebackCache::move_version_from_dirty_to_cache(
                &self.dirty.objects,
                &self.cached.object_cache,
                *object_id,
                *version,
                &ObjectEntry::Wrapped,
            );
        }
    }

    // Updates the latest object id cache with an entry that was read from the db.
    fn cache_latest_object_by_id(
        &self,
        object_id: &ObjectID,
        object: LatestObjectCacheEntry,
        ticket: Ticket,
    ) {
        trace!("caching object by id: {:?} {:?}", object_id, object);
        if self
            .object_by_id_cache
            .insert(object_id, object, ticket)
            .is_ok()
        {
            self.metrics.record_cache_write("object_by_id");
        } else {
            trace!("discarded cache write due to expired ticket");
            self.metrics.record_ticket_expiry();
        }
    }

    fn cache_object_not_found(&self, object_id: &ObjectID, ticket: Ticket) {
        self.cache_latest_object_by_id(object_id, LatestObjectCacheEntry::NonExistent, ticket);
    }

    fn clear_state_end_of_epoch_impl(&self, _execution_guard: &ExecutionLockWriteGuard<'_>) {
        info!("clearing state at end of epoch");
        assert!(
            self.dirty.pending_transaction_writes.is_empty(),
            "should be empty due to revert_state_update"
        );
        self.dirty.clear();
        info!("clearing old transaction locks");
        self.object_locks.clear();
    }

    fn revert_state_update_impl(&self, tx: &TransactionDigest) {
        // TODO: remove revert_state_update_impl entirely, and simply drop all dirty
        // state when clear_state_end_of_epoch_impl is called.
        // Further, once we do this, we can delay the insertion of the transaction into
        // pending_consensus_transactions until after the transaction has executed.
        let Some((_, outputs)) = self.dirty.pending_transaction_writes.remove(tx) else {
            assert!(
                !self.is_tx_already_executed(tx),
                "attempt to revert committed transaction"
            );

            // A transaction can be inserted into pending_consensus_transactions, but then reconfiguration
            // can happen before the transaction executes.
            info!("Not reverting {:?} as it was not executed", tx);
            return;
        };

        for (object_id, object) in outputs.written.iter() {
            if object.is_package() {
                info!("removing non-finalized package from cache: {:?}", object_id);
                self.packages.invalidate(object_id);
            }
            self.object_by_id_cache.invalidate(object_id);
            self.cached.object_cache.invalidate(object_id);
        }

        for ObjectKey(object_id, _) in outputs.deleted.iter().chain(outputs.wrapped.iter()) {
            self.object_by_id_cache.invalidate(object_id);
            self.cached.object_cache.invalidate(object_id);
        }

        // Note: individual object entries are removed when clear_state_end_of_epoch_impl is called
    }

    pub fn clear_caches_and_assert_empty(&self) {
        info!("clearing caches");
        self.cached.clear_and_assert_empty();
        self.object_by_id_cache.invalidate_all();
        assert!(&self.object_by_id_cache.is_empty());
        self.packages.invalidate_all();
        assert_empty(&self.packages);
    }
}

impl ObjectCacheRead for ReadonlyWritebackCache {
    fn get_package_object(&self, package_id: &ObjectID) -> SuiResult<Option<PackageObject>> {
        self.metrics
            .record_cache_request("package", "package_cache");
        if let Some(p) = self.packages.get(package_id) {
            if cfg!(debug_assertions) {
                let canonical_package = self
                    .dirty
                    .objects
                    .get(package_id)
                    .and_then(|v| match v.get_highest().map(|v| v.1.clone()) {
                        Some(ObjectEntry::Object(object)) => Some(object),
                        _ => None,
                    })
                    .or_else(|| self.store.get_object(package_id));

                if let Some(canonical_package) = canonical_package {
                    assert_eq!(
                        canonical_package.digest(),
                        p.object().digest(),
                        "Package object cache is inconsistent for package {:?}",
                        package_id
                    );
                }
            }
            self.metrics.record_cache_hit("package", "package_cache");
            return Ok(Some(p));
        } else {
            self.metrics.record_cache_miss("package", "package_cache");
        }

        // We try the dirty objects cache as well before going to the database. This is necessary
        // because the package could be evicted from the package cache before it is committed
        // to the database.
        if let Some(p) = self.get_object_impl("package", package_id) {
            if p.is_package() {
                let p = PackageObject::new(p);
                tracing::trace!(
                    "caching package: {:?}",
                    p.object().compute_object_reference()
                );
                self.metrics.record_cache_write("package");
                self.packages.insert(*package_id, p.clone());
                Ok(Some(p))
            } else {
                Err(SuiError::UserInputError {
                    error: UserInputError::MoveObjectAsPackage {
                        object_id: *package_id,
                    },
                })
            }
        } else {
            Ok(None)
        }
    }

    fn force_reload_system_packages(&self, _system_package_ids: &[ObjectID]) {
        // This is a no-op because all writes go through the cache, therefore it can never
        // be incoherent
    }

    // get_object and variants.

    fn get_object(&self, id: &ObjectID) -> Option<Object> {
        self.get_object_impl("object_latest", id)
    }

    fn get_object_by_key(&self, object_id: &ObjectID, version: SequenceNumber) -> Option<Object> {
        match self.get_object_by_key_cache_only(object_id, version) {
            CacheResult::Hit(object) => Some(object),
            CacheResult::NegativeHit => None,
            CacheResult::Miss => self
                .record_db_get("object_by_version")
                .get_object_by_key(object_id, version),
        }
    }

    fn multi_get_objects_by_key(&self, object_keys: &[ObjectKey]) -> Vec<Option<Object>> {
        do_fallback_lookup(
            object_keys,
            |key| match self.get_object_by_key_cache_only(&key.0, key.1) {
                CacheResult::Hit(maybe_object) => CacheResult::Hit(Some(maybe_object)),
                CacheResult::NegativeHit => CacheResult::NegativeHit,
                CacheResult::Miss => CacheResult::Miss,
            },
            |remaining| {
                self.record_db_multi_get("object_by_version", remaining.len())
                    .multi_get_objects_by_key(remaining)
                    .expect("db error")
            },
        )
    }

    fn object_exists_by_key(&self, object_id: &ObjectID, version: SequenceNumber) -> bool {
        match self.get_object_by_key_cache_only(object_id, version) {
            CacheResult::Hit(_) => true,
            CacheResult::NegativeHit => false,
            CacheResult::Miss => self
                .record_db_get("object_by_version")
                .object_exists_by_key(object_id, version)
                .expect("db error"),
        }
    }

    fn multi_object_exists_by_key(&self, object_keys: &[ObjectKey]) -> Vec<bool> {
        do_fallback_lookup(
            object_keys,
            |key| match self.get_object_by_key_cache_only(&key.0, key.1) {
                CacheResult::Hit(_) => CacheResult::Hit(true),
                CacheResult::NegativeHit => CacheResult::Hit(false),
                CacheResult::Miss => CacheResult::Miss,
            },
            |remaining| {
                self.record_db_multi_get("object_by_version", remaining.len())
                    .multi_object_exists_by_key(remaining)
                    .expect("db error")
            },
        )
    }

    fn get_latest_object_ref_or_tombstone(&self, object_id: ObjectID) -> Option<ObjectRef> {
        match self.get_object_entry_by_id_cache_only("latest_objref_or_tombstone", &object_id) {
            CacheResult::Hit((version, entry)) => Some(match entry {
                ObjectEntry::Object(object) => object.compute_object_reference(),
                ObjectEntry::Deleted => (object_id, version, ObjectDigest::OBJECT_DIGEST_DELETED),
                ObjectEntry::Wrapped => (object_id, version, ObjectDigest::OBJECT_DIGEST_WRAPPED),
            }),
            CacheResult::NegativeHit => None,
            CacheResult::Miss => self
                .record_db_get("latest_objref_or_tombstone")
                .get_latest_object_ref_or_tombstone(object_id)
                .expect("db error"),
        }
    }

    fn get_latest_object_or_tombstone(
        &self,
        object_id: ObjectID,
    ) -> Option<(ObjectKey, ObjectOrTombstone)> {
        match self.get_object_entry_by_id_cache_only("latest_object_or_tombstone", &object_id) {
            CacheResult::Hit((version, entry)) => {
                let key = ObjectKey(object_id, version);
                Some(match entry {
                    ObjectEntry::Object(object) => (key, object.into()),
                    ObjectEntry::Deleted => (
                        key,
                        ObjectOrTombstone::Tombstone((
                            object_id,
                            version,
                            ObjectDigest::OBJECT_DIGEST_DELETED,
                        )),
                    ),
                    ObjectEntry::Wrapped => (
                        key,
                        ObjectOrTombstone::Tombstone((
                            object_id,
                            version,
                            ObjectDigest::OBJECT_DIGEST_WRAPPED,
                        )),
                    ),
                })
            }
            CacheResult::NegativeHit => None,
            CacheResult::Miss => self
                .record_db_get("latest_object_or_tombstone")
                .get_latest_object_or_tombstone(object_id)
                .expect("db error"),
        }
    }

    fn multi_input_objects_available_cache_only(&self, keys: &[InputKey]) -> Vec<bool> {
        keys.iter()
            .map(|key| {
                if key.is_cancelled() {
                    true
                } else {
                    match key {
                        InputKey::VersionedObject { id, version } => {
                            matches!(
                                self.get_object_by_key_cache_only(&id.id(), *version),
                                CacheResult::Hit(_)
                            )
                        }
                        InputKey::Package { id } => self.packages.contains_key(id),
                    }
                }
            })
            .collect()
    }

    #[instrument(level = "trace", skip_all, fields(object_id, version_bound))]
    fn find_object_lt_or_eq_version(
        &self,
        object_id: ObjectID,
        version_bound: SequenceNumber,
    ) -> Option<Object> {
        macro_rules! check_cache_entry {
            ($level: expr, $objects: expr) => {
                self.metrics
                    .record_cache_request("object_lt_or_eq_version", $level);
                if let Some(objects) = $objects {
                    if let Some((_, object)) = objects
                        .all_versions_lt_or_eq_descending(&version_bound)
                        .next()
                    {
                        if let ObjectEntry::Object(object) = object {
                            self.metrics
                                .record_cache_hit("object_lt_or_eq_version", $level);
                            return Some(object.clone());
                        } else {
                            // if we find a tombstone, the object does not exist
                            self.metrics
                                .record_cache_negative_hit("object_lt_or_eq_version", $level);
                            return None;
                        }
                    } else {
                        self.metrics
                            .record_cache_miss("object_lt_or_eq_version", $level);
                    }
                }
            };
        }

        // if we have the latest version cached, and it is within the bound, we are done
        self.metrics
            .record_cache_request("object_lt_or_eq_version", "object_by_id");
        let latest_cache_entry = self.object_by_id_cache.get(&object_id);
        if let Some(latest) = &latest_cache_entry {
            let latest = latest.lock();
            match &*latest {
                LatestObjectCacheEntry::Object(latest_version, object) => {
                    if *latest_version <= version_bound {
                        if let ObjectEntry::Object(object) = object {
                            self.metrics
                                .record_cache_hit("object_lt_or_eq_version", "object_by_id");
                            return Some(object.clone());
                        } else {
                            // object is a tombstone, but is still within the version bound
                            self.metrics.record_cache_negative_hit(
                                "object_lt_or_eq_version",
                                "object_by_id",
                            );
                            return None;
                        }
                    }
                    // latest object is not within the version bound. fall through.
                }
                // No object by this ID exists at all
                LatestObjectCacheEntry::NonExistent => {
                    self.metrics
                        .record_cache_negative_hit("object_lt_or_eq_version", "object_by_id");
                    return None;
                }
            }
        }
        self.metrics
            .record_cache_miss("object_lt_or_eq_version", "object_by_id");

        Self::with_locked_cache_entries(
            &self.dirty.objects,
            &self.cached.object_cache,
            &object_id,
            |dirty_entry, cached_entry| {
                check_cache_entry!("committed", dirty_entry);
                check_cache_entry!("uncommitted", cached_entry);

                // Much of the time, the query will be for the very latest object version, so
                // try that first. But we have to be careful:
                // 1. We must load the tombstone if it is present, because its version may exceed
                //    the version_bound, in which case we must do a scan.
                // 2. You might think we could just call `self.store.get_latest_object_or_tombstone` here.
                //    But we cannot, because there may be a more recent version in the dirty set, which
                //    we skipped over in check_cache_entry! because of the version bound. However, if we
                //    skipped it above, we will skip it here as well, again due to the version bound.
                // 3. Despite that, we really want to warm the cache here. Why? Because if the object is
                //    cold (not being written to), then we will very soon be able to start serving reads
                //    of it from the object_by_id cache, IF we can warm the cache. If we don't warm the
                //    the cache here, and no writes to the object occur, then we will always have to go
                //    to the db for the object.
                //
                // Lastly, it is important to understand the rationale for all this: If the object is
                // write-hot, we will serve almost all reads to it from the dirty set (or possibly the
                // cached set if it is only written to once every few checkpoints). If the object is
                // write-cold (or non-existent) and read-hot, then we will serve almost all reads to it
                // from the object_by_id cache check above.  Most of the apparently wasteful code here
                // exists only to ensure correctness in all the edge cases.
                let latest: Option<(SequenceNumber, ObjectEntry)> =
                    if let Some(dirty_set) = dirty_entry {
                        dirty_set
                            .get_highest()
                            .cloned()
                            .tap_none(|| panic!("dirty set cannot be empty"))
                    } else {
                        // TODO: we should try not to read from the db while holding the locks.
                        self.record_db_get("object_lt_or_eq_version_latest")
                            .get_latest_object_or_tombstone(object_id)
                            .expect("db error")
                            .map(|(ObjectKey(_, version), obj_or_tombstone)| {
                                (version, ObjectEntry::from(obj_or_tombstone))
                            })
                    };

                if let Some((obj_version, obj_entry)) = latest {
                    // we can always cache the latest object (or tombstone), even if it is not within the
                    // version_bound. This is done in order to warm the cache in the case where a sequence
                    // of transactions all read the same child object without writing to it.

                    // Note: no need to call with_object_by_id_cache_update here, because we are holding
                    // the lock on the dirty cache entry, and `latest` cannot become out-of-date
                    // while we hold that lock.
                    self.cache_latest_object_by_id(
                        &object_id,
                        LatestObjectCacheEntry::Object(obj_version, obj_entry.clone()),
                        // We can get a ticket at the last second, because we are holding the lock
                        // on dirty, so there cannot be any concurrent writes.
                        self.object_by_id_cache.get_ticket_for_read(&object_id),
                    );

                    if obj_version <= version_bound {
                        match obj_entry {
                            ObjectEntry::Object(object) => Some(object),
                            ObjectEntry::Deleted | ObjectEntry::Wrapped => None,
                        }
                    } else {
                        // The latest object exceeded the bound, so now we have to do a scan
                        // But we already know there is no dirty entry within the bound,
                        // so we go to the db.
                        self.record_db_get("object_lt_or_eq_version_scan")
                            .find_object_lt_or_eq_version(object_id, version_bound)
                            .expect("db error")
                    }

                // no object found in dirty set or db, object does not exist
                // When this is called from a read api (i.e. not the execution path) it is
                // possible that the object has been deleted and pruned. In this case,
                // there would be no entry at all on disk, but we may have a tombstone in the
                // cache
                } else if let Some(latest_cache_entry) = latest_cache_entry {
                    // If there is a latest cache entry, it had better not be a live object!
                    assert!(!latest_cache_entry.lock().is_alive());
                    None
                } else {
                    // If there is no latest cache entry, we can insert one.
                    let highest = cached_entry.and_then(|c| c.get_highest());
                    assert!(highest.is_none() || highest.unwrap().1.is_tombstone());
                    self.cache_object_not_found(
                        &object_id,
                        // okay to get ticket at last second - see above
                        self.object_by_id_cache.get_ticket_for_read(&object_id),
                    );
                    None
                }
            },
        )
    }

    fn get_sui_system_state_object_unsafe(&self) -> SuiResult<SuiSystemState> {
        get_sui_system_state(self)
    }

    fn get_bridge_object_unsafe(&self) -> SuiResult<Bridge> {
        get_bridge(self)
    }

    fn get_marker_value(
        &self,
        object_key: FullObjectKey,
        epoch_id: EpochId,
    ) -> Option<MarkerValue> {
        match self.get_marker_value_cache_only(object_key, epoch_id) {
            CacheResult::Hit(marker) => Some(marker),
            CacheResult::NegativeHit => None,
            CacheResult::Miss => self
                .record_db_get("marker_by_version")
                .get_marker_value(object_key, epoch_id)
                .expect("db error"),
        }
    }

    fn get_latest_marker(
        &self,
        object_id: FullObjectID,
        epoch_id: EpochId,
    ) -> Option<(SequenceNumber, MarkerValue)> {
        match self.get_latest_marker_value_cache_only(object_id, epoch_id) {
            CacheResult::Hit((v, marker)) => Some((v, marker)),
            CacheResult::NegativeHit => {
                panic!("cannot have negative hit when getting latest marker")
            }
            CacheResult::Miss => self
                .record_db_get("marker_latest")
                .get_latest_marker(object_id, epoch_id)
                .expect("db error"),
        }
    }

    fn get_lock(&self, obj_ref: ObjectRef, epoch_store: &AuthorityPerEpochStore) -> SuiLockResult {
        let cur_epoch = epoch_store.epoch();
        match self.get_object_by_id_cache_only("lock", &obj_ref.0) {
            CacheResult::Hit((_, obj)) => {
                let actual_objref = obj.compute_object_reference();
                if obj_ref != actual_objref {
                    Ok(ObjectLockStatus::LockedAtDifferentVersion {
                        locked_ref: actual_objref,
                    })
                } else {
                    // requested object ref is live, check if there is a lock
                    Ok(
                        match self
                            .object_locks
                            .get_transaction_lock(&obj_ref, epoch_store)?
                        {
                            Some(tx_digest) => ObjectLockStatus::LockedToTx {
                                locked_by_tx: LockDetailsDeprecated {
                                    epoch: cur_epoch,
                                    tx_digest,
                                },
                            },
                            None => ObjectLockStatus::Initialized,
                        },
                    )
                }
            }
            CacheResult::NegativeHit => {
                Err(SuiError::from(UserInputError::ObjectNotFound {
                    object_id: obj_ref.0,
                    // even though we know the requested version, we leave it as None to indicate
                    // that the object does not exist at any version
                    version: None,
                }))
            }
            CacheResult::Miss => self.record_db_get("lock").get_lock(obj_ref, epoch_store),
        }
    }

    fn _get_live_objref(&self, object_id: ObjectID) -> SuiResult<ObjectRef> {
        let obj = self.get_object_impl("live_objref", &object_id).ok_or(
            UserInputError::ObjectNotFound {
                object_id,
                version: None,
            },
        )?;
        Ok(obj.compute_object_reference())
    }

    fn check_owned_objects_are_live(&self, owned_object_refs: &[ObjectRef]) -> SuiResult {
        do_fallback_lookup_fallible(
            owned_object_refs,
            |obj_ref| match self.get_object_by_id_cache_only("object_is_live", &obj_ref.0) {
                CacheResult::Hit((version, obj)) => {
                    if obj.compute_object_reference() != *obj_ref {
                        Err(UserInputError::ObjectVersionUnavailableForConsumption {
                            provided_obj_ref: *obj_ref,
                            current_version: version,
                        }
                        .into())
                    } else {
                        Ok(CacheResult::Hit(()))
                    }
                }
                CacheResult::NegativeHit => Err(UserInputError::ObjectNotFound {
                    object_id: obj_ref.0,
                    version: None,
                }
                .into()),
                CacheResult::Miss => Ok(CacheResult::Miss),
            },
            |remaining| {
                self.record_db_multi_get("object_is_live", remaining.len())
                    .check_owned_objects_are_live(remaining)?;
                Ok(vec![(); remaining.len()])
            },
        )?;
        Ok(())
    }

    fn get_highest_pruned_checkpoint(&self) -> Option<CheckpointSequenceNumber> {
        self.store
            .perpetual_tables
            .get_highest_pruned_checkpoint()
            .expect("db error")
    }

    fn notify_read_input_objects<'a>(
        &'a self,
        input_and_receiving_keys: &'a [InputKey],
        receiving_keys: &'a HashSet<InputKey>,
        epoch: &'a EpochId,
    ) -> BoxFuture<'a, ()> {
        self.object_notify_read
            .read(input_and_receiving_keys, |keys| {
                self.multi_input_objects_available(keys, receiving_keys, epoch)
                    .into_iter()
                    .map(|available| if available { Some(()) } else { None })
                    .collect::<Vec<_>>()
            })
            .map(|_| ())
            .boxed()
    }
}

impl TransactionCacheRead for ReadonlyWritebackCache {
    fn multi_get_transaction_blocks(
        &self,
        digests: &[TransactionDigest],
    ) -> Vec<Option<Arc<VerifiedTransaction>>> {
        let digests_and_tickets: Vec<_> = digests
            .iter()
            .map(|d| (*d, self.cached.transactions.get_ticket_for_read(d)))
            .collect();
        do_fallback_lookup(
            &digests_and_tickets,
            |(digest, _)| {
                self.metrics
                    .record_cache_request("transaction_block", "uncommitted");
                if let Some(tx) = self.dirty.pending_transaction_writes.get(digest) {
                    self.metrics
                        .record_cache_hit("transaction_block", "uncommitted");
                    return CacheResult::Hit(Some(tx.transaction.clone()));
                }
                self.metrics
                    .record_cache_miss("transaction_block", "uncommitted");

                self.metrics
                    .record_cache_request("transaction_block", "committed");

                match self
                    .cached
                    .transactions
                    .get(digest)
                    .map(|l| l.lock().clone())
                {
                    Some(PointCacheItem::Some(tx)) => {
                        self.metrics
                            .record_cache_hit("transaction_block", "committed");
                        CacheResult::Hit(Some(tx))
                    }
                    Some(PointCacheItem::None) => CacheResult::NegativeHit,
                    None => {
                        self.metrics
                            .record_cache_miss("transaction_block", "committed");

                        CacheResult::Miss
                    }
                }
            },
            |remaining| {
                let remaining_digests: Vec<_> = remaining.iter().map(|(d, _)| *d).collect();
                let results: Vec<_> = self
                    .record_db_multi_get("transaction_block", remaining.len())
                    .multi_get_transaction_blocks(&remaining_digests)
                    .expect("db error")
                    .into_iter()
                    .map(|o| o.map(Arc::new))
                    .collect();
                for ((digest, ticket), result) in remaining.iter().zip(results.iter()) {
                    if result.is_none() {
                        self.cached.transactions.insert(digest, None, *ticket).ok();
                    }
                }
                results
            },
        )
    }

    fn multi_get_executed_effects_digests(
        &self,
        digests: &[TransactionDigest],
    ) -> Vec<Option<TransactionEffectsDigest>> {
        let digests_and_tickets: Vec<_> = digests
            .iter()
            .map(|d| {
                (
                    *d,
                    self.cached.executed_effects_digests.get_ticket_for_read(d),
                )
            })
            .collect();
        do_fallback_lookup(
            &digests_and_tickets,
            |(digest, _)| {
                self.metrics
                    .record_cache_request("executed_effects_digests", "uncommitted");
                if let Some(digest) = self.dirty.executed_effects_digests.get(digest) {
                    self.metrics
                        .record_cache_hit("executed_effects_digests", "uncommitted");
                    return CacheResult::Hit(Some(*digest));
                }
                self.metrics
                    .record_cache_miss("executed_effects_digests", "uncommitted");

                self.metrics
                    .record_cache_request("executed_effects_digests", "committed");
                match self
                    .cached
                    .executed_effects_digests
                    .get(digest)
                    .map(|l| *l.lock())
                {
                    Some(PointCacheItem::Some(digest)) => {
                        self.metrics
                            .record_cache_hit("executed_effects_digests", "committed");
                        CacheResult::Hit(Some(digest))
                    }
                    Some(PointCacheItem::None) => CacheResult::NegativeHit,
                    None => {
                        self.metrics
                            .record_cache_miss("executed_effects_digests", "committed");
                        CacheResult::Miss
                    }
                }
            },
            |remaining| {
                let remaining_digests: Vec<_> = remaining.iter().map(|(d, _)| *d).collect();
                let results = self
                    .record_db_multi_get("executed_effects_digests", remaining.len())
                    .multi_get_executed_effects_digests(&remaining_digests)
                    .expect("db error");
                for ((digest, ticket), result) in remaining.iter().zip(results.iter()) {
                    if result.is_none() {
                        self.cached
                            .executed_effects_digests
                            .insert(digest, None, *ticket)
                            .ok();
                    }
                }
                results
            },
        )
    }

    fn multi_get_effects(
        &self,
        digests: &[TransactionEffectsDigest],
    ) -> Vec<Option<TransactionEffects>> {
        let digests_and_tickets: Vec<_> = digests
            .iter()
            .map(|d| (*d, self.cached.transaction_effects.get_ticket_for_read(d)))
            .collect();
        do_fallback_lookup(
            &digests_and_tickets,
            |(digest, _)| {
                self.metrics
                    .record_cache_request("transaction_effects", "uncommitted");
                if let Some(effects) = self.dirty.transaction_effects.get(digest) {
                    self.metrics
                        .record_cache_hit("transaction_effects", "uncommitted");
                    return CacheResult::Hit(Some(effects.clone()));
                }
                self.metrics
                    .record_cache_miss("transaction_effects", "uncommitted");

                self.metrics
                    .record_cache_request("transaction_effects", "committed");
                match self
                    .cached
                    .transaction_effects
                    .get(digest)
                    .map(|l| l.lock().clone())
                {
                    Some(PointCacheItem::Some(effects)) => {
                        self.metrics
                            .record_cache_hit("transaction_effects", "committed");
                        CacheResult::Hit(Some((*effects).clone()))
                    }
                    Some(PointCacheItem::None) => CacheResult::NegativeHit,
                    None => {
                        self.metrics
                            .record_cache_miss("transaction_effects", "committed");
                        CacheResult::Miss
                    }
                }
            },
            |remaining| {
                let remaining_digests: Vec<_> = remaining.iter().map(|(d, _)| *d).collect();
                let results = self
                    .record_db_multi_get("transaction_effects", remaining.len())
                    .multi_get_effects(remaining_digests.iter())
                    .expect("db error");
                for ((digest, ticket), result) in remaining.iter().zip(results.iter()) {
                    if result.is_none() {
                        self.cached
                            .transaction_effects
                            .insert(digest, None, *ticket)
                            .ok();
                    }
                }
                results
            },
        )
    }

    fn notify_read_executed_effects_digests<'a>(
        &'a self,
        digests: &'a [TransactionDigest],
    ) -> BoxFuture<'a, Vec<TransactionEffectsDigest>> {
        self.executed_effects_digests_notify_read
            .read(digests, |digests| {
                self.multi_get_executed_effects_digests(digests)
            })
            .boxed()
    }

    fn multi_get_events(
        &self,
        event_digests: &[TransactionDigest],
    ) -> Vec<Option<TransactionEvents>> {
        fn map_events(events: TransactionEvents) -> Option<TransactionEvents> {
            if events.data.is_empty() {
                None
            } else {
                Some(events)
            }
        }

        let digests_and_tickets: Vec<_> = event_digests
            .iter()
            .map(|d| (*d, self.cached.transaction_events.get_ticket_for_read(d)))
            .collect();
        do_fallback_lookup(
            &digests_and_tickets,
            |(digest, _)| {
                self.metrics
                    .record_cache_request("transaction_events", "uncommitted");
                if let Some(events) = self.dirty.transaction_events.get(digest).map(|e| e.clone()) {
                    self.metrics
                        .record_cache_hit("transaction_events", "uncommitted");

                    return CacheResult::Hit(map_events(events));
                }
                self.metrics
                    .record_cache_miss("transaction_events", "uncommitted");

                self.metrics
                    .record_cache_request("transaction_events", "committed");
                match self
                    .cached
                    .transaction_events
                    .get(digest)
                    .map(|l| l.lock().clone())
                {
                    Some(PointCacheItem::Some(events)) => {
                        self.metrics
                            .record_cache_hit("transaction_events", "committed");
                        CacheResult::Hit(map_events((*events).clone()))
                    }
                    Some(PointCacheItem::None) => CacheResult::NegativeHit,
                    None => {
                        self.metrics
                            .record_cache_miss("transaction_events", "committed");

                        CacheResult::Miss
                    }
                }
            },
            |remaining| {
                let remaining_digests: Vec<_> = remaining.iter().map(|(d, _)| *d).collect();
                let results = self
                    .store
                    .multi_get_events(&remaining_digests)
                    .expect("db error");
                for ((digest, ticket), result) in remaining.iter().zip(results.iter()) {
                    if result.is_none() {
                        self.cached
                            .transaction_events
                            .insert(digest, None, *ticket)
                            .ok();
                    }
                }
                results
            },
        )
    }

    fn is_tx_fastpath_executed(&self, tx_digest: &TransactionDigest) -> bool {
        self.dirty
            .fastpath_transaction_outputs
            .contains_key(tx_digest)
    }

    fn notify_read_fastpath_transaction_outputs<'a>(
        &'a self,
        tx_digests: &'a [TransactionDigest],
    ) -> BoxFuture<'a, Vec<Arc<TransactionOutputs>>> {
        self.fastpath_transaction_outputs_notify_read
            .read(tx_digests, |tx_digests| {
                tx_digests
                    .iter()
                    .map(|tx_digest| {
                        self.dirty
                            .fastpath_transaction_outputs
                            .get(tx_digest)
                            .clone()
                    })
                    .collect()
            })
            .boxed()
    }
}
