use std::sync::Arc;

use sui_types::{
    base_types::{ObjectID, ObjectRef, SequenceNumber, VersionNumber},
    committee::EpochId,
    digests::{ObjectDigest, TransactionDigest, TransactionEffectsDigest, TransactionEventsDigest},
    effects::{TransactionEffects, TransactionEvents},
    error::{SuiError, SuiResult},
    event::Event,
    global_state_hash::GlobalStateHash,
    messages_checkpoint::CheckpointSequenceNumber,
    object::Object,
    storage::{FullObjectKey, MarkerValue, ObjectKey, ObjectStore},
    sui_system_state::epoch_start_sui_system_state::EpochStartSystemStateTrait,
    transaction::TrustedTransaction,
};
use typed_store::{rocks::DBMap, Map};

use super::{
    authority_store::LockDetailsWrapperDeprecated,
    authority_store_tables::{
        AuthorityPerpetualTables, AuthorityPerpetualTablesReadOnly, LiveSetIter,
    },
    authority_store_types::{
        try_construct_object, StoreObject, StoreObjectValue, StoreObjectWrapper,
    },
    epoch_start_configuration::{EpochStartConfigTrait, EpochStartConfiguration},
};

#[derive(Clone)]
pub enum AuthorityPerpetualTablesMix {
    Full(Arc<AuthorityPerpetualTables>),
    ReadOnly(Arc<AuthorityPerpetualTablesReadOnly>),
}

impl AuthorityPerpetualTablesMix {
    pub fn inner(&self) -> Arc<AuthorityPerpetualTables> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => tables.clone(),
            AuthorityPerpetualTablesMix::ReadOnly(_) => {
                panic!("Attempted to access full tables from read-only mix")
            }
        }
    }

    pub fn object(
        &self,
        object_key: &ObjectKey,
        store_object: StoreObjectWrapper,
    ) -> Result<Option<Object>, SuiError> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => tables.object(object_key, store_object),
            AuthorityPerpetualTablesMix::ReadOnly(tables) => {
                tables.object(object_key, store_object)
            }
        }
    }

    pub fn as_ref(&self) -> &Self {
        &self
    }

    pub fn live_owned_object_markers(
        &self,
    ) -> &DBMap<ObjectRef, Option<LockDetailsWrapperDeprecated>> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => &tables.live_owned_object_markers,
            AuthorityPerpetualTablesMix::ReadOnly(tables) => &tables.live_owned_object_markers,
        }
    }

    pub fn tombstone_reference(
        &self,
        object_key: &ObjectKey,
        store_object: &StoreObjectWrapper,
    ) -> Result<Option<ObjectRef>, SuiError> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => {
                tables.tombstone_reference(object_key, store_object)
            }
            AuthorityPerpetualTablesMix::ReadOnly(tables) => {
                tables.object_reference(object_key, store_object)
            }
        }
    }

    pub fn get_latest_object_ref_or_tombstone(
        &self,
        object_id: ObjectID,
    ) -> Result<Option<ObjectRef>, SuiError> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => {
                tables.get_latest_object_ref_or_tombstone(object_id)
            }
            AuthorityPerpetualTablesMix::ReadOnly(tables) => {
                tables.get_latest_object_ref_or_tombstone(object_id)
            }
        }
    }

    pub fn get_latest_object_or_tombstone(
        &self,
        object_id: ObjectID,
    ) -> Result<Option<(ObjectKey, StoreObjectWrapper)>, SuiError> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => {
                tables.get_latest_object_or_tombstone(object_id)
            }
            AuthorityPerpetualTablesMix::ReadOnly(tables) => {
                tables.get_latest_object_or_tombstone(object_id)
            }
        }
    }

    pub fn find_object_lt_or_eq_version(
        &self,
        object_id: ObjectID,
        version: SequenceNumber,
    ) -> SuiResult<Option<Object>> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => {
                tables.find_object_lt_or_eq_version(object_id, version)
            }
            AuthorityPerpetualTablesMix::ReadOnly(tables) => {
                tables.find_object_lt_or_eq_version(object_id, version)
            }
        }
    }

    pub fn epoch_start_configuration(&self) -> &DBMap<(), EpochStartConfiguration> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => &tables.epoch_start_configuration,
            AuthorityPerpetualTablesMix::ReadOnly(tables) => &tables.epoch_start_configuration,
        }
    }

    pub fn iter_live_object_set(&self, include_wrapped_object: bool) -> LiveSetIter<'_> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => {
                tables.iter_live_object_set(include_wrapped_object)
            }
            AuthorityPerpetualTablesMix::ReadOnly(tables) => {
                tables.iter_live_object_set(include_wrapped_object)
            }
        }
    }

    pub fn get_object_by_key(
        &self,
        object_id: &ObjectID,
        version: VersionNumber,
    ) -> Option<Object> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => {
                tables.get_object_by_key(object_id, version)
            }
            AuthorityPerpetualTablesMix::ReadOnly(tables) => {
                tables.get_object_by_key(object_id, version)
            }
        }
    }

    pub fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => tables.get_object(object_id),
            AuthorityPerpetualTablesMix::ReadOnly(tables) => tables.get_object(object_id),
        }
    }

    pub fn root_state_hash_by_epoch(
        &self,
    ) -> &DBMap<EpochId, (CheckpointSequenceNumber, GlobalStateHash)> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => &tables.root_state_hash_by_epoch,
            AuthorityPerpetualTablesMix::ReadOnly(tables) => &tables.root_state_hash_by_epoch,
        }
    }

    pub fn executed_transactions_to_checkpoint(
        &self,
    ) -> &DBMap<TransactionDigest, (EpochId, CheckpointSequenceNumber)> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => {
                &tables.executed_transactions_to_checkpoint
            }
            AuthorityPerpetualTablesMix::ReadOnly(tables) => {
                &tables.executed_transactions_to_checkpoint
            }
        }
    }

    pub fn database_is_empty(&self) -> SuiResult<bool> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => tables.database_is_empty(),
            AuthorityPerpetualTablesMix::ReadOnly(tables) => tables.database_is_empty(),
        }
    }

    pub fn events(&self) -> &DBMap<(TransactionEventsDigest, usize), Event> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => &tables.events,
            AuthorityPerpetualTablesMix::ReadOnly(tables) => &tables.events,
        }
    }

    pub fn events_2(&self) -> &DBMap<TransactionDigest, TransactionEvents> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => &tables.events_2,
            AuthorityPerpetualTablesMix::ReadOnly(tables) => &tables.events_2,
        }
    }

    pub fn effects(&self) -> &DBMap<TransactionEffectsDigest, TransactionEffects> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => &tables.effects,
            AuthorityPerpetualTablesMix::ReadOnly(tables) => &tables.effects,
        }
    }

    pub fn transactions(&self) -> &DBMap<TransactionDigest, TrustedTransaction> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => &tables.transactions,
            AuthorityPerpetualTablesMix::ReadOnly(tables) => &tables.transactions,
        }
    }

    pub fn get_recovery_epoch_at_restart(&self) -> SuiResult<EpochId> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => tables.get_recovery_epoch_at_restart(),
            AuthorityPerpetualTablesMix::ReadOnly(tables) => tables.get_recovery_epoch_at_restart(),
        }
    }

    pub fn objects(&self) -> &DBMap<ObjectKey, StoreObjectWrapper> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => &tables.objects,
            AuthorityPerpetualTablesMix::ReadOnly(tables) => &tables.objects,
        }
    }

    pub fn object_reference(
        &self,
        object_key: &ObjectKey,
        store_object: StoreObjectWrapper,
    ) -> Result<ObjectRef, SuiError> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => {
                tables.object_reference(object_key, store_object)
            }
            AuthorityPerpetualTablesMix::ReadOnly(tables) => {
                tables.object_reference(object_key, store_object)
            }
        }
    }

    pub fn object_per_epoch_marker_table(&self) -> &DBMap<(EpochId, ObjectKey), MarkerValue> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => &tables.object_per_epoch_marker_table,
            AuthorityPerpetualTablesMix::ReadOnly(tables) => &tables.object_per_epoch_marker_table,
        }
    }

    pub fn object_per_epoch_marker_table_v2(
        &self,
    ) -> &DBMap<(EpochId, FullObjectKey), MarkerValue> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => &tables.object_per_epoch_marker_table_v2,
            AuthorityPerpetualTablesMix::ReadOnly(tables) => {
                &tables.object_per_epoch_marker_table_v2
            }
        }
    }

    pub fn expected_storage_fund_imbalance(&self) -> &DBMap<(), i64> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => &tables.expected_storage_fund_imbalance,
            AuthorityPerpetualTablesMix::ReadOnly(tables) => {
                &tables.expected_storage_fund_imbalance
            }
        }
    }

    pub fn expected_network_sui_amount(&self) -> &DBMap<(), u64> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => &tables.expected_network_sui_amount,
            AuthorityPerpetualTablesMix::ReadOnly(tables) => &tables.expected_network_sui_amount,
        }
    }

    pub fn executed_effects(&self) -> &DBMap<TransactionDigest, TransactionEffectsDigest> {
        match self {
            AuthorityPerpetualTablesMix::Full(tables) => &tables.executed_effects,
            AuthorityPerpetualTablesMix::ReadOnly(tables) => &tables.executed_effects,
        }
    }
}

impl AuthorityPerpetualTablesReadOnly {
    pub fn get_recovery_epoch_at_restart(&self) -> SuiResult<EpochId> {
        Ok(self
            .epoch_start_configuration
            .get(&())?
            .expect("Must have current epoch.")
            .epoch_start_state()
            .epoch())
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
}
