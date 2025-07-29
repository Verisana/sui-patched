use super::*;
use crate::authority::authority_store_types::{
    try_construct_object, StoreObject, StoreObjectValue, StoreObjectWrapper,
};
use authority_store_tables::{AuthorityPerpetualTablesReadOnly, LiveObject};
use std::path::Path;
use sui_types::base_types::SequenceNumber;
use sui_types::effects::TransactionEffects;
use sui_types::global_state_hash::GlobalStateHash;
use typed_store::traits::Map;
use typed_store::DbIterator;

impl AuthorityPerpetualTablesReadOnly {
    pub fn path(parent_path: &Path) -> PathBuf {
        parent_path.join("perpetual")
    }

    // This is used by indexer to find the correct version of dynamic field child object.
    // We do not store the version of the child object, but because of lamport timestamp,
    // we know the child must have version number less then or eq to the parent.
    pub fn find_object_lt_or_eq_version(
        &self,
        object_id: ObjectID,
        version: SequenceNumber,
    ) -> SuiResult<Option<Object>> {
        let mut iter = self.objects.reversed_safe_iter_with_bounds(
            Some(ObjectKey::min_for_id(&object_id)),
            Some(ObjectKey(object_id, version)),
        )?;
        match iter.next() {
            Some(Ok((key, o))) => self.object(&key, o),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    fn construct_object(
        &self,
        object_key: &ObjectKey,
        store_object: StoreObjectValue,
    ) -> Result<Object, SuiError> {
        try_construct_object(object_key, store_object)
    }

    // Constructs `sui_types::object::Object` from `StoreObjectWrapper`.
    // Returns `None` if object was deleted/wrapped
    pub fn object(
        &self,
        object_key: &ObjectKey,
        store_object: StoreObjectWrapper,
    ) -> Result<Option<Object>, SuiError> {
        let StoreObject::Value(store_object) = store_object.migrate().into_inner() else {
            return Ok(None);
        };
        Ok(Some(self.construct_object(object_key, store_object)?))
    }

    pub fn object_reference(
        &self,
        object_key: &ObjectKey,
        store_object: StoreObjectWrapper,
    ) -> Result<ObjectRef, SuiError> {
        let obj_ref = match store_object.migrate().into_inner() {
            StoreObject::Value(object) => self
                .construct_object(object_key, object)?
                .compute_object_reference(),
            StoreObject::Deleted => (
                object_key.0,
                object_key.1,
                ObjectDigest::OBJECT_DIGEST_DELETED,
            ),
            StoreObject::Wrapped => (
                object_key.0,
                object_key.1,
                ObjectDigest::OBJECT_DIGEST_WRAPPED,
            ),
        };
        Ok(obj_ref)
    }

    pub fn tombstone_reference(
        &self,
        object_key: &ObjectKey,
        store_object: &StoreObjectWrapper,
    ) -> Result<Option<ObjectRef>, SuiError> {
        let obj_ref = match store_object.inner() {
            StoreObject::Deleted => Some((
                object_key.0,
                object_key.1,
                ObjectDigest::OBJECT_DIGEST_DELETED,
            )),
            StoreObject::Wrapped => Some((
                object_key.0,
                object_key.1,
                ObjectDigest::OBJECT_DIGEST_WRAPPED,
            )),
            _ => None,
        };
        Ok(obj_ref)
    }

    pub fn get_latest_object_ref_or_tombstone(
        &self,
        object_id: ObjectID,
    ) -> Result<Option<ObjectRef>, SuiError> {
        let mut iterator = self.objects.reversed_safe_iter_with_bounds(
            Some(ObjectKey::min_for_id(&object_id)),
            Some(ObjectKey::max_for_id(&object_id)),
        )?;

        if let Some(Ok((object_key, value))) = iterator.next() {
            if object_key.0 == object_id {
                return Ok(Some(self.object_reference(&object_key, value)?));
            }
        }
        Ok(None)
    }

    pub fn get_latest_object_or_tombstone(
        &self,
        object_id: ObjectID,
    ) -> Result<Option<(ObjectKey, StoreObjectWrapper)>, SuiError> {
        let mut iterator = self.objects.reversed_safe_iter_with_bounds(
            Some(ObjectKey::min_for_id(&object_id)),
            Some(ObjectKey::max_for_id(&object_id)),
        )?;

        if let Some(Ok((object_key, value))) = iterator.next() {
            if object_key.0 == object_id {
                return Ok(Some((object_key, value)));
            }
        }
        Ok(None)
    }

    pub fn get_recovery_epoch_at_restart(&self) -> SuiResult<EpochId> {
        Ok(self
            .epoch_start_configuration
            .get(&())?
            .expect("Must have current epoch.")
            .epoch_start_state()
            .epoch())
    }

    pub fn get_highest_pruned_checkpoint(
        &self,
    ) -> Result<Option<CheckpointSequenceNumber>, TypedStoreError> {
        self.pruned_checkpoint.get(&())
    }

    pub fn get_transaction(
        &self,
        digest: &TransactionDigest,
    ) -> SuiResult<Option<TrustedTransaction>> {
        let Some(transaction) = self.transactions.get(digest)? else {
            return Ok(None);
        };
        Ok(Some(transaction))
    }

    pub fn get_effects(&self, digest: &TransactionDigest) -> SuiResult<Option<TransactionEffects>> {
        let Some(effect_digest) = self.executed_effects.get(digest)? else {
            return Ok(None);
        };
        Ok(self.effects.get(&effect_digest)?)
    }

    // DEPRECATED as the backing table has been moved to authority_per_epoch_store.
    // Please do not add new accessors/callsites.
    pub fn get_checkpoint_sequence_number(
        &self,
        digest: &TransactionDigest,
    ) -> SuiResult<Option<(EpochId, CheckpointSequenceNumber)>> {
        Ok(self.executed_transactions_to_checkpoint.get(digest)?)
    }

    pub fn get_newer_object_keys(
        &self,
        object: &(ObjectID, SequenceNumber),
    ) -> SuiResult<Vec<ObjectKey>> {
        let mut objects = vec![];
        for result in self.objects.safe_iter_with_bounds(
            Some(ObjectKey(object.0, object.1.next())),
            Some(ObjectKey(object.0, VersionNumber::MAX)),
        ) {
            let (key, _) = result?;
            objects.push(key);
        }
        Ok(objects)
    }

    pub fn database_is_empty(&self) -> SuiResult<bool> {
        Ok(self.objects.safe_iter().next().is_none())
    }

    pub fn iter_live_object_set(&self, include_wrapped_object: bool) -> LiveSetIter<'_> {
        LiveSetIter {
            iter: Box::new(self.objects.safe_iter()),
            tables: self,
            prev: None,
            include_wrapped_object,
        }
    }

    pub fn range_iter_live_object_set(
        &self,
        lower_bound: Option<ObjectID>,
        upper_bound: Option<ObjectID>,
        include_wrapped_object: bool,
    ) -> LiveSetIter<'_> {
        let lower_bound = lower_bound.as_ref().map(ObjectKey::min_for_id);
        let upper_bound = upper_bound.as_ref().map(ObjectKey::max_for_id);

        LiveSetIter {
            iter: Box::new(self.objects.safe_iter_with_bounds(lower_bound, upper_bound)),
            tables: self,
            prev: None,
            include_wrapped_object,
        }
    }

    pub fn checkpoint_db(&self, path: &Path) -> SuiResult {
        // This checkpoints the entire db and not just objects table
        self.objects.checkpoint_db(path).map_err(Into::into)
    }

    pub fn get_root_state_hash(
        &self,
        epoch: EpochId,
    ) -> SuiResult<Option<(CheckpointSequenceNumber, GlobalStateHash)>> {
        Ok(self.root_state_hash_by_epoch.get(&epoch)?)
    }

    // fallible get object methods for sui-tool, which may need to attempt to read a corrupted database
    pub fn get_object_fallible(&self, object_id: &ObjectID) -> SuiResult<Option<Object>> {
        let obj_entry = self
            .objects
            .reversed_safe_iter_with_bounds(None, Some(ObjectKey::max_for_id(object_id)))?
            .next();

        match obj_entry.transpose()? {
            Some((ObjectKey(obj_id, version), obj)) if obj_id == *object_id => {
                Ok(self.object(&ObjectKey(obj_id, version), obj)?)
            }
            _ => Ok(None),
        }
    }

    pub fn get_object_by_key_fallible(
        &self,
        object_id: &ObjectID,
        version: VersionNumber,
    ) -> SuiResult<Option<Object>> {
        Ok(self
            .objects
            .get(&ObjectKey(*object_id, version))?
            .and_then(|object| {
                self.object(&ObjectKey(*object_id, version), object)
                    .expect("object construction error")
            }))
    }
}

impl ObjectStore for AuthorityPerpetualTablesReadOnly {
    /// Read an object and return it, or Ok(None) if the object was not found.
    fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        self.get_object_fallible(object_id).expect("db error")
    }

    fn get_object_by_key(&self, object_id: &ObjectID, version: VersionNumber) -> Option<Object> {
        self.get_object_by_key_fallible(object_id, version)
            .expect("db error")
    }
}

pub struct LiveSetIter<'a> {
    iter: DbIterator<'a, (ObjectKey, StoreObjectWrapper)>,
    tables: &'a AuthorityPerpetualTablesReadOnly,
    prev: Option<(ObjectKey, StoreObjectWrapper)>,
    /// Whether a wrapped object is considered as a live object.
    include_wrapped_object: bool,
}

impl LiveSetIter<'_> {
    fn store_object_wrapper_to_live_object(
        &self,
        object_key: ObjectKey,
        store_object: StoreObjectWrapper,
    ) -> Option<LiveObject> {
        match store_object.migrate().into_inner() {
            StoreObject::Value(object) => {
                let object = self
                    .tables
                    .construct_object(&object_key, object)
                    .expect("Constructing object from store cannot fail");
                Some(LiveObject::Normal(object))
            }
            StoreObject::Wrapped => {
                if self.include_wrapped_object {
                    Some(LiveObject::Wrapped(object_key))
                } else {
                    None
                }
            }
            StoreObject::Deleted => None,
        }
    }
}

impl Iterator for LiveSetIter<'_> {
    type Item = LiveObject;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(Ok((next_key, next_value))) = self.iter.next() {
                let prev = self.prev.take();
                self.prev = Some((next_key, next_value));

                if let Some((prev_key, prev_value)) = prev {
                    if prev_key.0 != next_key.0 {
                        let live_object =
                            self.store_object_wrapper_to_live_object(prev_key, prev_value);
                        if live_object.is_some() {
                            return live_object;
                        }
                    }
                }
                continue;
            }
            if let Some((key, value)) = self.prev.take() {
                let live_object = self.store_object_wrapper_to_live_object(key, value);
                if live_object.is_some() {
                    return live_object;
                }
            }
            return None;
        }
    }
}
