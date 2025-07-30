use super::cache_types::Ticket;
use super::readonly_execution_cache::ReadonlyCacheInvalidator;
use super::writeback_cache::{
    CachedCommittedData, LatestObjectCacheEntry, ObjectEntry, UncommittedData,
};
use super::{
    cache_types::{CacheResult, CachedVersionMap, MonotonicCache},
    ExecutionCacheMetrics, ObjectCacheRead,
};
use crate::authority::authority_per_epoch_store::AuthorityPerEpochStore;
use crate::authority::authority_store::SuiLockResult;
use crate::authority::readonly_authority_store::ReadonlyAuthorityStore;
use crate::{check_cache_entry_by_latest, check_cache_entry_by_version};
use dashmap::mapref::entry::Entry as DashMapEntry;
use dashmap::DashMap;
use futures::future::BoxFuture;
use moka::sync::SegmentedCache as MokaCache;
use mysten_common::random_util::randomize_cache_capacity_in_tests;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;
use sui_config::ExecutionCacheConfig;
use sui_types::base_types::{EpochId, FullObjectID, ObjectID, ObjectRef, SequenceNumber};
use sui_types::bridge::Bridge;
use sui_types::digests::ObjectDigest;
use sui_types::error::{SuiError, SuiResult, UserInputError};
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use sui_types::object::Object;
use sui_types::storage::{
    FullObjectKey, InputKey, MarkerValue, ObjectKey, ObjectOrTombstone, ObjectStore, PackageObject,
};
use sui_types::sui_system_state::SuiSystemState;
use tracing::{instrument, trace, warn};

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

    store: Arc<ReadonlyAuthorityStore>,
    metrics: Arc<ExecutionCacheMetrics>,

    long_term_cache: MokaCache<ObjectID, LatestObjectCacheEntry>,
}

impl ReadonlyWritebackCache {
    fn check_long_term_cache(&self, id: &ObjectID) -> Option<Option<Object>> {
        match self.long_term_cache.get(id) {
            Some(latest) => match latest {
                LatestObjectCacheEntry::Object(_, obj_entry) => match obj_entry {
                    ObjectEntry::Object(obj) => Some(Some(obj)),
                    ObjectEntry::Deleted | ObjectEntry::Wrapped => Some(None),
                },
                LatestObjectCacheEntry::NonExistent => None,
            },
            None => None,
        }
    }

    fn cache_long_term_obj(&self, id: &ObjectID, entry: &Option<(ObjectKey, ObjectOrTombstone)>) {
        let value = match entry {
            Some((key, obj_or_tombstone)) => match obj_or_tombstone {
                ObjectOrTombstone::Object(obj) => {
                    LatestObjectCacheEntry::Object(key.1, ObjectEntry::Object(obj.clone()))
                }
                ObjectOrTombstone::Tombstone((_, sequence, digest)) => {
                    if digest == &ObjectDigest::OBJECT_DIGEST_DELETED {
                        LatestObjectCacheEntry::Object(*sequence, ObjectEntry::Deleted)
                    } else {
                        LatestObjectCacheEntry::Object(*sequence, ObjectEntry::Wrapped)
                    }
                }
            },
            None => LatestObjectCacheEntry::NonExistent,
        };

        self.long_term_cache.insert(*id, value);
    }
}

impl ReadonlyWritebackCache {
    pub fn new(
        config: &ExecutionCacheConfig,
        store: Arc<ReadonlyAuthorityStore>,
        metrics: Arc<ExecutionCacheMetrics>,
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
            store,
            metrics,
            long_term_cache: MokaCache::builder(8)
                .max_capacity(randomize_cache_capacity_in_tests(
                    config.object_cache_size(),
                ))
                .build(),
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

    fn get_object_impl(&self, request_type: &'static str, id: &ObjectID) -> Option<Object> {
        let ticket = self.object_by_id_cache.get_ticket_for_read(id);
        match self.get_object_entry_by_id_cache_only(request_type, id) {
            CacheResult::Hit((_, entry)) => match entry {
                ObjectEntry::Object(object) => Some(object),
                ObjectEntry::Deleted | ObjectEntry::Wrapped => None,
            },
            CacheResult::NegativeHit => None,
            CacheResult::Miss => {
                if let Some(object) = self.check_long_term_cache(id) {
                    return object;
                }
                let obj = self
                    .store
                    .get_latest_object_or_tombstone(*id)
                    .expect("db error");
                self.cache_long_term_obj(id, &obj);
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
            CacheResult::Miss => self.store.get_object_by_key(object_id, version),
        }
    }

    fn multi_get_objects_by_key(&self, _object_keys: &[ObjectKey]) -> Vec<Option<Object>> {
        panic!("multi_get_objects_by_key should not be called on ReadonlyWritebackCache");
    }

    fn object_exists_by_key(&self, _object_id: &ObjectID, _version: SequenceNumber) -> bool {
        panic!("object_exists_by_key should not be called on ReadonlyWritebackCache");
    }

    fn multi_object_exists_by_key(&self, _object_keys: &[ObjectKey]) -> Vec<bool> {
        panic!("multi_object_exists_by_key should not be called on ReadonlyWritebackCache");
    }

    fn get_latest_object_ref_or_tombstone(&self, _object_id: ObjectID) -> Option<ObjectRef> {
        panic!("get_latest_object_ref_or_tombstone should not be called on ReadonlyWritebackCache");
    }

    fn get_latest_object_or_tombstone(
        &self,
        _object_id: ObjectID,
    ) -> Option<(ObjectKey, ObjectOrTombstone)> {
        panic!("get_latest_object_or_tombstone should not be called on ReadonlyWritebackCache");
    }

    fn multi_input_objects_available_cache_only(&self, _keys: &[InputKey]) -> Vec<bool> {
        panic!("multi_input_objects_available_cache_only should not be called on ReadonlyWritebackCache");
    }

    #[instrument(level = "trace", skip_all, fields(object_id, version_bound))]
    fn find_object_lt_or_eq_version(
        &self,
        _object_id: ObjectID,
        _version_bound: SequenceNumber,
    ) -> Option<Object> {
        panic!("find_object_lt_or_eq_version should not be called on ReadonlyWritebackCache");
    }

    fn get_sui_system_state_object_unsafe(&self) -> SuiResult<SuiSystemState> {
        panic!("get_sui_system_state_object_unsafe should not be called on ReadonlyWritebackCache");
    }

    fn get_bridge_object_unsafe(&self) -> SuiResult<Bridge> {
        panic!("get_bridge_object_unsafe should not be called on ReadonlyWritebackCache");
    }

    fn get_marker_value(
        &self,
        _object_key: FullObjectKey,
        _epoch_id: EpochId,
    ) -> Option<MarkerValue> {
        panic!("get_marker_value should not be called on ReadonlyWritebackCache");
    }

    fn get_latest_marker(
        &self,
        _object_id: FullObjectID,
        _epoch_id: EpochId,
    ) -> Option<(SequenceNumber, MarkerValue)> {
        panic!("get_latest_marker should not be called on ReadonlyWritebackCache");
    }

    fn get_lock(
        &self,
        _obj_ref: ObjectRef,
        _epoch_store: &AuthorityPerEpochStore,
    ) -> SuiLockResult {
        panic!("get_lock should not be called on ReadonlyWritebackCache");
    }

    fn _get_live_objref(&self, _object_id: ObjectID) -> SuiResult<ObjectRef> {
        panic!("_get_live_objref should not be called on ReadonlyWritebackCache");
    }

    fn check_owned_objects_are_live(&self, _owned_object_refs: &[ObjectRef]) -> SuiResult {
        panic!("check_owned_objects_are_live should not be called on ReadonlyWritebackCache");
    }

    fn get_highest_pruned_checkpoint(&self) -> Option<CheckpointSequenceNumber> {
        panic!("get_highest_pruned_checkpoint should not be called on ReadonlyWritebackCache");
    }

    fn notify_read_input_objects<'a>(
        &'a self,
        _input_and_receiving_keys: &'a [InputKey],
        _receiving_keys: &'a HashSet<InputKey>,
        _epoch: &'a EpochId,
    ) -> BoxFuture<'a, ()> {
        panic!("notify_read_input_objects should not be called on ReadonlyWritebackCache");
    }
}

impl ReadonlyCacheInvalidator for ReadonlyWritebackCache {
    fn invalidate_objects(&self, _object_ids: &[ObjectID]) -> SuiResult {
        Ok(())
    }

    fn invalidate_all_objects(&self) -> SuiResult {
        Ok(())
    }
}
