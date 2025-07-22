use super::*;
use crate::authority::authority_per_epoch_store::AuthorityPerEpochStore;
use authority_store::{AuthorityStoreMetrics, LockDetailsDeprecated, SuiLockResult};
use authority_store_tables::AuthorityPerpetualTablesReadOnly;
use std::ops::Not;
use std::sync::Arc;
use sui_types::digests::TransactionEventsDigest;
use sui_types::effects::{TransactionEffects, TransactionEvents};
use sui_types::error::UserInputError;
use sui_types::storage::{FullObjectKey, MarkerValue, ObjectKey, ObjectOrTombstone, ObjectStore};
use sui_types::{base_types::SequenceNumber, fp_bail};
use typed_store::traits::Map;
use typed_store::TypedStoreError;

/// ALL_OBJ_VER determines whether we want to store all past
/// versions of every object in the store. Authority doesn't store
/// them, but other entities such as replicas will.
/// S is a template on Authority signature state. This allows SuiDataStore to be used on either
/// authorities or non-authorities. Specifically, when storing transactions and effects,
/// S allows SuiDataStore to either store the authority signed version or unsigned version.
pub struct ReadonlyAuthorityStore {
    pub(crate) perpetual_tables: Arc<AuthorityPerpetualTablesReadOnly>,
}

impl ReadonlyAuthorityStore {
    pub fn open_readonly(
        perpetual_tables: Arc<AuthorityPerpetualTablesReadOnly>,
        registry: &Registry,
    ) -> SuiResult<Arc<Self>> {
        let store = Arc::new(Self { perpetual_tables });
        Ok(store)
    }

    pub fn get_events_by_events_digest(
        &self,
        event_digest: &TransactionEventsDigest,
    ) -> Result<Option<TransactionEvents>, TypedStoreError> {
        let data = self
            .perpetual_tables
            .events
            .safe_range_iter((*event_digest, 0)..=(*event_digest, usize::MAX))
            .map_ok(|(_, event)| event)
            .collect::<Result<Vec<_>, TypedStoreError>>()?;
        Ok(data.is_empty().not().then_some(TransactionEvents { data }))
    }

    pub fn get_executed_effects(
        &self,
        tx_digest: &TransactionDigest,
    ) -> Result<Option<TransactionEffects>, TypedStoreError> {
        let effects_digest = self.perpetual_tables.executed_effects.get(tx_digest)?;
        match effects_digest {
            Some(digest) => Ok(self.perpetual_tables.effects.get(&digest)?),
            None => Ok(None),
        }
    }

    pub fn get_marker_value(
        &self,
        object_key: FullObjectKey,
        epoch_id: EpochId,
    ) -> SuiResult<Option<MarkerValue>> {
        Ok(self
            .perpetual_tables
            .object_per_epoch_marker_table_v2
            .get(&(epoch_id, object_key))?)
    }

    pub fn get_latest_marker(
        &self,
        object_id: FullObjectID,
        epoch_id: EpochId,
    ) -> SuiResult<Option<(SequenceNumber, MarkerValue)>> {
        let min_key = (epoch_id, FullObjectKey::min_for_id(&object_id));
        let max_key = (epoch_id, FullObjectKey::max_for_id(&object_id));

        let marker_entry = self
            .perpetual_tables
            .object_per_epoch_marker_table_v2
            .reversed_safe_iter_with_bounds(Some(min_key), Some(max_key))?
            .next();
        match marker_entry {
            Some(Ok(((epoch, key), marker))) => {
                // because of the iterator bounds these cannot fail
                assert_eq!(epoch, epoch_id);
                assert_eq!(key.id(), object_id);
                Ok(Some((key.version(), marker)))
            }
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    pub fn object_exists_by_key(
        &self,
        object_id: &ObjectID,
        version: VersionNumber,
    ) -> SuiResult<bool> {
        Ok(self
            .perpetual_tables
            .objects
            .contains_key(&ObjectKey(*object_id, version))?)
    }

    pub fn multi_object_exists_by_key(&self, object_keys: &[ObjectKey]) -> SuiResult<Vec<bool>> {
        Ok(self
            .perpetual_tables
            .objects
            .multi_contains_keys(object_keys.to_vec())?
            .into_iter()
            .collect())
    }

    pub fn multi_get_objects_by_key(
        &self,
        object_keys: &[ObjectKey],
    ) -> Result<Vec<Option<Object>>, SuiError> {
        let wrappers = self
            .perpetual_tables
            .objects
            .multi_get(object_keys.to_vec())?;
        let mut ret = vec![];

        for (idx, w) in wrappers.into_iter().enumerate() {
            ret.push(
                w.map(|object| self.perpetual_tables.object(&object_keys[idx], object))
                    .transpose()?
                    .flatten(),
            );
        }
        Ok(ret)
    }

    /// Gets ObjectLockInfo that represents state of lock on an object.
    /// Returns UserInputError::ObjectNotFound if cannot find lock record for this object
    pub(crate) fn get_lock(
        &self,
        obj_ref: ObjectRef,
        epoch_store: &AuthorityPerEpochStore,
    ) -> SuiLockResult {
        if self
            .perpetual_tables
            .live_owned_object_markers
            .get(&obj_ref)?
            .is_none()
        {
            return Ok(ObjectLockStatus::LockedAtDifferentVersion {
                locked_ref: self.get_latest_live_version_for_object_id(obj_ref.0)?,
            });
        }

        let tables = epoch_store.tables()?;
        let epoch_id = epoch_store.epoch();

        if let Some(tx_digest) = tables.get_locked_transaction(&obj_ref)? {
            Ok(ObjectLockStatus::LockedToTx {
                locked_by_tx: LockDetailsDeprecated {
                    epoch: epoch_id,
                    tx_digest,
                },
            })
        } else {
            Ok(ObjectLockStatus::Initialized)
        }
    }

    /// Returns UserInputError::ObjectNotFound if no lock records found for this object.
    pub(crate) fn get_latest_live_version_for_object_id(
        &self,
        object_id: ObjectID,
    ) -> SuiResult<ObjectRef> {
        let mut iterator = self
            .perpetual_tables
            .live_owned_object_markers
            .reversed_safe_iter_with_bounds(
                None,
                Some((object_id, SequenceNumber::MAX, ObjectDigest::MAX)),
            )?;
        Ok(iterator
            .next()
            .transpose()?
            .and_then(|value| {
                if value.0 .0 == object_id {
                    Some(value)
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                SuiError::from(UserInputError::ObjectNotFound {
                    object_id,
                    version: None,
                })
            })?
            .0)
    }

    /// Checks multiple object locks exist.
    /// Returns UserInputError::ObjectNotFound if cannot find lock record for at least one of the objects.
    /// Returns UserInputError::ObjectVersionUnavailableForConsumption if at least one object lock is not initialized
    ///     at the given version.
    pub fn check_owned_objects_are_live(&self, objects: &[ObjectRef]) -> SuiResult {
        let locks = self
            .perpetual_tables
            .live_owned_object_markers
            .multi_get(objects)?;
        for (lock, obj_ref) in locks.into_iter().zip(objects) {
            if lock.is_none() {
                let latest_lock = self.get_latest_live_version_for_object_id(obj_ref.0)?;
                fp_bail!(UserInputError::ObjectVersionUnavailableForConsumption {
                    provided_obj_ref: *obj_ref,
                    current_version: latest_lock.1
                }
                .into());
            }
        }
        Ok(())
    }

    /// Return the object with version less then or eq to the provided seq number.
    /// This is used by indexer to find the correct version of dynamic field child object.
    /// We do not store the version of the child object, but because of lamport timestamp,
    /// we know the child must have version number less then or eq to the parent.
    pub fn find_object_lt_or_eq_version(
        &self,
        object_id: ObjectID,
        version: SequenceNumber,
    ) -> SuiResult<Option<Object>> {
        self.perpetual_tables
            .find_object_lt_or_eq_version(object_id, version)
    }

    /// Returns the latest object reference we have for this object_id in the objects table.
    ///
    /// The method may also return the reference to a deleted object with a digest of
    /// ObjectDigest::deleted() or ObjectDigest::wrapped() and lamport version
    /// of a transaction that deleted the object.
    /// Note that a deleted object may re-appear if the deletion was the result of the object
    /// being wrapped in another object.
    ///
    /// If no entry for the object_id is found, return None.
    pub fn get_latest_object_ref_or_tombstone(
        &self,
        object_id: ObjectID,
    ) -> Result<Option<ObjectRef>, SuiError> {
        self.perpetual_tables
            .get_latest_object_ref_or_tombstone(object_id)
    }

    /// Returns the latest object we have for this object_id in the objects table.
    ///
    /// If no entry for the object_id is found, return None.
    pub fn get_latest_object_or_tombstone(
        &self,
        object_id: ObjectID,
    ) -> Result<Option<(ObjectKey, ObjectOrTombstone)>, SuiError> {
        let Some((object_key, store_object)) = self
            .perpetual_tables
            .get_latest_object_or_tombstone(object_id)?
        else {
            return Ok(None);
        };

        if let Some(object_ref) = self
            .perpetual_tables
            .tombstone_reference(&object_key, &store_object)?
        {
            return Ok(Some((object_key, ObjectOrTombstone::Tombstone(object_ref))));
        }

        let object = self
            .perpetual_tables
            .object(&object_key, store_object)?
            .expect("Non tombstone store object could not be converted to object");

        Ok(Some((object_key, ObjectOrTombstone::Object(object))))
    }
}

impl ObjectStore for ReadonlyAuthorityStore {
    /// Read an object and return it, or Ok(None) if the object was not found.
    fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        self.perpetual_tables.as_ref().get_object(object_id)
    }

    fn get_object_by_key(&self, object_id: &ObjectID, version: VersionNumber) -> Option<Object> {
        self.perpetual_tables.get_object_by_key(object_id, version)
    }
}
