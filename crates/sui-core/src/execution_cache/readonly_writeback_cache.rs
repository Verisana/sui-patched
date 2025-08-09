use super::readonly_execution_cache::ReadonlyCacheInvalidator;
use super::writeback_cache::{LatestObjectCacheEntry, ObjectEntry};
use super::ObjectCacheRead;
use crate::authority::authority_per_epoch_store::AuthorityPerEpochStore;
use crate::authority::authority_store::SuiLockResult;
use crate::authority::readonly_authority_store::ReadonlyAuthorityStore;
use dashmap::DashMap;
use futures::future::BoxFuture;
use moka::sync::SegmentedCache as MokaCache;
use mysten_common::random_util::randomize_cache_capacity_in_tests;
use std::collections::HashSet;
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
use tracing::{instrument, warn};

pub struct ReadonlyWritebackCache {
    store: Arc<ReadonlyAuthorityStore>,

    packages: MokaCache<ObjectID, Option<PackageObject>>,

    obj_cache: MokaCache<ObjectID, LatestObjectCacheEntry>,
    obj_lt_eq_version: MokaCache<ObjectID, DashMap<SequenceNumber, Option<Object>>>,
    obj_by_key: MokaCache<ObjectID, DashMap<SequenceNumber, Option<Object>>>,
}

impl ReadonlyWritebackCache {
    fn check_long_term_cache(&self, id: &ObjectID) -> Option<Option<Object>> {
        match self.obj_cache.get(id) {
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

        self.obj_cache.insert(*id, value);
    }

    fn check_long_term_lt_eq_version_cache(
        &self,
        id: &ObjectID,
        version: SequenceNumber,
    ) -> Option<Option<Object>> {
        self.obj_lt_eq_version
            .get(id)?
            .get(&version)
            .map(|entry| entry.clone())
    }

    fn cache_long_term_lt_eq_version(
        &self,
        id: &ObjectID,
        version: SequenceNumber,
        entry: &Option<Object>,
    ) {
        match self.obj_lt_eq_version.get(id) {
            Some(entries) => {
                entries.insert(version, entry.clone());
            }
            None => {
                self.obj_lt_eq_version
                    .insert(*id, DashMap::from_iter([(version, entry.clone())]));
            }
        }
    }

    fn check_package_cache(&self, id: &ObjectID) -> Option<Option<PackageObject>> {
        self.packages.get(id)
    }

    fn cache_package(&self, id: ObjectID, package: Option<PackageObject>) {
        self.packages.insert(id, package);
    }

    fn check_long_term_object_by_key(
        &self,
        id: &ObjectID,
        version: SequenceNumber,
    ) -> Option<Option<Object>> {
        self.obj_by_key
            .get(id)?
            .get(&version)
            .map(|entry| entry.clone())
    }

    fn cache_long_term_object_by_key(
        &self,
        id: &ObjectID,
        version: SequenceNumber,
        entry: &Option<Object>,
    ) {
        match self.obj_by_key.get(id) {
            Some(entries) => {
                entries.insert(version, entry.clone());
            }
            None => {
                self.obj_by_key
                    .insert(*id, DashMap::from_iter([(version, entry.clone())]));
            }
        }
    }

    fn get_object_by_key_impl(
        &self,
        object_id: &ObjectID,
        version: SequenceNumber,
    ) -> Option<Object> {
        if let Some(object) = self.check_long_term_object_by_key(object_id, version) {
            return object;
        }
        let obj = self.store.get_object_by_key(object_id, version);
        self.cache_long_term_object_by_key(object_id, version, &obj);

        obj
    }
}

impl ReadonlyWritebackCache {
    pub fn new(config: &ExecutionCacheConfig, store: Arc<ReadonlyAuthorityStore>) -> Self {
        let packages = MokaCache::builder(8)
            .max_capacity(randomize_cache_capacity_in_tests(
                config.package_cache_size(),
            ))
            .build();
        Self {
            packages,
            store,
            obj_cache: MokaCache::builder(8)
                .max_capacity(randomize_cache_capacity_in_tests(
                    config.object_cache_size(),
                ))
                .build(),
            obj_lt_eq_version: MokaCache::builder(8)
                .max_capacity(randomize_cache_capacity_in_tests(
                    config.object_cache_size(),
                ))
                .build(),
            obj_by_key: MokaCache::builder(8)
                .max_capacity(randomize_cache_capacity_in_tests(
                    config.object_cache_size(),
                ))
                .build(),
        }
    }

    fn get_object_impl(&self, id: &ObjectID) -> Option<Object> {
        if let Some(object) = self.check_long_term_cache(id) {
            return object;
        }
        let obj = self
            .store
            .get_latest_object_or_tombstone(*id)
            .expect("db error");
        self.cache_long_term_obj(id, &obj);
        match obj {
            Some((_, obj)) => match obj {
                ObjectOrTombstone::Object(object) => Some(object),
                ObjectOrTombstone::Tombstone(_) => None,
            },
            None => None,
        }
    }
}

impl ObjectCacheRead for ReadonlyWritebackCache {
    fn get_package_object(&self, package_id: &ObjectID) -> SuiResult<Option<PackageObject>> {
        if let Some(package) = self.check_package_cache(package_id) {
            return Ok(package);
        }

        let package = if let Some(p) = self.get_object_impl(package_id) {
            if p.is_package() {
                Some(PackageObject::new(p))
            } else {
                return Err(SuiError::UserInputError {
                    error: UserInputError::MoveObjectAsPackage {
                        object_id: *package_id,
                    },
                });
            }
        } else {
            None
        };
        self.cache_package(*package_id, package.clone());

        Ok(package)
    }

    fn force_reload_system_packages(&self, _system_package_ids: &[ObjectID]) {
        // This is a no-op because all writes go through the cache, therefore it can never
        // be incoherent
    }

    // get_object and variants.

    fn get_object(&self, id: &ObjectID) -> Option<Object> {
        self.get_object_impl(id)
    }

    fn get_object_by_key(&self, object_id: &ObjectID, version: SequenceNumber) -> Option<Object> {
        self.get_object_by_key_impl(object_id, version)
    }

    fn multi_get_objects_by_key(&self, object_keys: &[ObjectKey]) -> Vec<Option<Object>> {
        object_keys
            .iter()
            .map(|key| self.get_object_by_key_impl(&key.0, key.1))
            .collect()
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
        object_id: ObjectID,
        version_bound: SequenceNumber,
    ) -> Option<Object> {
        if let Some(object) = self.check_long_term_lt_eq_version_cache(&object_id, version_bound) {
            return object;
        }
        let obj = ReadonlyAuthorityStore::find_object_lt_or_eq_version(
            &self.store,
            object_id,
            version_bound,
        )
        .expect("db error");
        self.cache_long_term_lt_eq_version(&object_id, version_bound, &obj);
        obj
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
        _epoch: EpochId,
    ) -> BoxFuture<'a, ()> {
        panic!("notify_read_input_objects should not be called on ReadonlyWritebackCache");
    }
}

impl ReadonlyCacheInvalidator for ReadonlyWritebackCache {
    fn invalidate_objects(&self, object_ids: &[ObjectID]) -> SuiResult {
        object_ids.iter().for_each(|id| {
            self.obj_cache.invalidate(id);
            self.obj_lt_eq_version.invalidate(id);
            self.obj_by_key.invalidate(id);
        });
        Ok(())
    }

    fn invalidate_all_objects(&self) -> SuiResult {
        self.obj_by_key.invalidate_all();
        self.obj_lt_eq_version.invalidate_all();
        self.obj_cache.invalidate_all();
        Ok(())
    }
}
