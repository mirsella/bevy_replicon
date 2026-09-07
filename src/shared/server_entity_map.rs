use bevy::{ecs::entity::hash_map::EntityHashMap, prelude::*};
use log::{error, warn};

/// Maps server entities to client entities and vice versa.
///
/// Inserted as resource by [`ClientPlugin`](crate::client::ClientPlugin).
///
/// Automatically updated on replication receive. If the client predicts a despawn,
/// the mapping is kept until the server also despawns the entity.
#[derive(Resource, Default)]
pub struct ServerEntityMap {
    server_to_client: EntityHashMap<Entity>,
    client_to_server: EntityHashMap<Entity>,
}

impl ServerEntityMap {
    /// Inserts a server-client pair into the map.
    pub(crate) fn insert(&mut self, server_entity: Entity, client_entity: Entity) {
        if let Some(existing_entity) = self.server_to_client.insert(server_entity, client_entity) {
            if client_entity != existing_entity {
                error!(
                    "mapping {server_entity:?} to {client_entity:?}, but it's already mapped to {existing_entity:?}"
                );
                if self.client_to_server.get(&existing_entity) == Some(&server_entity) {
                    self.client_to_server.remove(&existing_entity);
                }
            } else {
                warn!("ignoring duplicate mapping from {server_entity:?} to {client_entity:?}");
            }
        }

        if let Some(existing_server_entity) =
            self.client_to_server.insert(client_entity, server_entity)
            && existing_server_entity != server_entity
            && self.server_to_client.get(&existing_server_entity) == Some(&client_entity)
        {
            self.server_to_client.remove(&existing_server_entity);
        }
    }

    /// Returns server to client mappings.
    #[inline]
    pub fn to_client(&self) -> &EntityHashMap<Entity> {
        &self.server_to_client
    }

    /// Returns client to server mappings.
    #[inline]
    pub fn to_server(&self) -> &EntityHashMap<Entity> {
        &self.client_to_server
    }

    /// Returns the mapped client entity, inserting one when absent.
    pub(crate) fn get_or_insert_with(
        &mut self,
        server_entity: Entity,
        f: impl FnOnce() -> Entity,
    ) -> Entity {
        if let Some(&client_entity) = self.server_to_client.get(&server_entity) {
            client_entity
        } else {
            let client_entity = f();
            self.insert(server_entity, client_entity);
            client_entity
        }
    }

    /// Clears the map.
    pub(crate) fn clear(&mut self) {
        self.client_to_server.clear();
        self.server_to_client.clear();
    }

    /// Removes a mapping by its client entity.
    pub(crate) fn remove_by_client(&mut self, client_entity: Entity) -> Option<Entity> {
        let server_entity = self.client_to_server.remove(&client_entity)?;
        if self.server_to_client.get(&server_entity) == Some(&client_entity) {
            self.server_to_client.remove(&server_entity);
        }
        Some(server_entity)
    }
}

#[cfg(test)]
mod tests {
    use test_log::test;

    use super::*;

    #[test]
    fn mapping() {
        const SERVER_ENTITY: Entity = Entity::from_raw_u32(0).unwrap();
        const CLIENT_ENTITY: Entity = Entity::from_raw_u32(1).unwrap();
        const SECOND_SERVER_ENTITY: Entity = Entity::from_raw_u32(2).unwrap();

        let mut map = ServerEntityMap::default();
        map.insert(SERVER_ENTITY, Entity::PLACEHOLDER);
        map.insert(SERVER_ENTITY, CLIENT_ENTITY);
        assert_eq!(map.to_client().get(&SERVER_ENTITY), Some(&CLIENT_ENTITY));
        assert!(!map.to_server().contains_key(&Entity::PLACEHOLDER));

        assert_eq!(
            map.get_or_insert_with(SERVER_ENTITY, || Entity::PLACEHOLDER),
            CLIENT_ENTITY
        );
        map.insert(SECOND_SERVER_ENTITY, CLIENT_ENTITY);
        assert!(!map.to_client().contains_key(&SERVER_ENTITY));
        assert_eq!(
            map.to_client().get(&SECOND_SERVER_ENTITY),
            Some(&CLIENT_ENTITY)
        );
        assert_eq!(
            map.to_server().get(&CLIENT_ENTITY),
            Some(&SECOND_SERVER_ENTITY)
        );

        assert_eq!(
            map.remove_by_client(CLIENT_ENTITY),
            Some(SECOND_SERVER_ENTITY)
        );
        assert!(map.to_client().is_empty());
        assert!(map.to_server().is_empty());
    }
}
