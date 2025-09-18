use super::readonly_writeback_cache::ReadonlyWritebackCache;
use super::ObjectCacheRead;
use crate::authority::readonly_authority_store::ReadonlyAuthorityStore;
use crate::implement_storage_traits;
use std::sync::Arc;
use sui_config::ExecutionCacheConfig;
use sui_types::base_types::{FullObjectID, ObjectID, ObjectRef, SequenceNumber};
use sui_types::committee::EpochId;
use sui_types::error::{SuiError, SuiResult};
use sui_types::object::{Object, Owner};
use sui_types::storage::{
    BackingPackageStore, BackingStore, ChildObjectResolver, FullObjectKey, ObjectStore,
    PackageObject, ParentSync,
};

pub trait ReadonlyCacheInvalidator {
    fn invalidate_objects(&self, object_ids: &[ObjectID]) -> SuiResult;

    fn invalidate_all_objects(&self) -> SuiResult;
}

// If you have Arc<ExecutionCache>, you cannot return a reference to it as
// an &Arc<dyn ExecutionCacheRead> (for example), because the trait object is a fat pointer.
// So, in order to be able to return &Arc<dyn T>, we create all the converted trait objects
// (aka fat pointers) up front and return references to them.
#[derive(Clone)]
pub struct ReadonlyExecutionCacheTraitPointers {
    pub object_cache_reader: Arc<dyn ObjectCacheRead>,
    pub backing_store: Arc<dyn BackingStore + Send + Sync>,
    pub backing_package_store: Arc<dyn BackingPackageStore + Send + Sync>,
    pub object_store: Arc<dyn ObjectStore + Send + Sync>,
    pub cache_invalidator: Arc<dyn ReadonlyCacheInvalidator + Send + Sync>,
}

impl ReadonlyExecutionCacheTraitPointers {
    pub fn new<T>(cache: Arc<T>) -> Self
    where
        T: ObjectCacheRead
            + BackingStore
            + BackingPackageStore
            + ObjectStore
            + ReadonlyCacheInvalidator
            + 'static,
    {
        Self {
            object_cache_reader: cache.clone(),
            backing_store: cache.clone(),
            backing_package_store: cache.clone(),
            object_store: cache.clone(),
            cache_invalidator: cache,
        }
    }
}

pub fn build_readonly_execution_cache(
    cache_config: &ExecutionCacheConfig,
    store: &Arc<ReadonlyAuthorityStore>,
) -> ReadonlyExecutionCacheTraitPointers {
    ReadonlyExecutionCacheTraitPointers::new(
        ReadonlyWritebackCache::new(cache_config, store.clone()).into(),
    )
}

implement_storage_traits!(ReadonlyWritebackCache);
