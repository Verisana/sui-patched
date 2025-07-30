use super::*;
use authority_store_tables::AuthorityPerpetualTablesReadOnly;
use std::sync::Arc;
use sui_types::storage::{ObjectKey, ObjectOrTombstone, ObjectStore};
use typed_store::Map;

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
    ) -> SuiResult<Arc<Self>> {
        let store = Arc::new(Self { perpetual_tables });
        Ok(store)
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
