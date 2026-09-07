pub mod confirm_history;
#[cfg(feature = "client_diagnostics")]
pub mod diagnostics;
pub mod message;
pub mod server_mutate_ticks;

use alloc::string::String;
use bevy::prelude::*;
use bytes::{Buf, Bytes};
use log::{Level, debug, error, log_enabled, trace};
use postcard::experimental::max_size::MaxSize;

use crate::{
    postcard_utils,
    prelude::*,
    shared::{
        backend::channels::{ClientChannel, ServerChannel},
        replication::{
            deferred_entity::{DeferredEntity, EntityScratch},
            message_flags::{MutateFlags, UpdateFlags},
            mutate_index::MutateIndex,
            receive_markers::{EntityMarkers, ReceiveMarkers},
            registry::{
                ReplicationRegistry,
                ctx::{BufferedSpawner, DespawnCtx, EntityBuffer, RemoveCtx, WriteCtx},
            },
            signature::SignatureMap,
            storage::ReplicationStorage,
        },
        server_entity_map::ServerEntityMap,
    },
};
use confirm_history::{ConfirmHistory, EntityReplicated};
use server_mutate_ticks::{MutateTickReceived, ServerMutateTicks};

/// Client functionality and replication receiving.
///
/// Can be disabled for server-only apps.
pub struct ClientPlugin;

impl Plugin for ClientPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<ClientMessages>()
            .init_resource::<ClientStats>()
            .init_resource::<ServerEntityMap>()
            .init_resource::<ServerUpdateTick>()
            .init_resource::<ServerMutateTicks>()
            .init_resource::<BufferedMutations>()
            .add_message::<EntityReplicated>()
            .add_message::<MutateTickReceived>()
            .add_message::<ClientReplicationError>()
            .configure_sets(
                PreUpdate,
                (
                    ClientSystems::ReceivePackets,
                    ClientSystems::Receive,
                    ClientSystems::Diagnostics,
                )
                    .chain(),
            )
            .configure_sets(
                OnEnter(ClientState::Connected),
                (ClientSystems::Receive, ClientSystems::Diagnostics).chain(),
            )
            .configure_sets(
                PostUpdate,
                (ClientSystems::Send, ClientSystems::SendPackets).chain(),
            )
            .add_observer(cleanup_storage)
            .add_observer(cleanup_entity_map)
            .add_systems(
                PreUpdate,
                receive_replication
                    .in_set(ClientSystems::Receive)
                    .run_if(in_state(ClientState::Connected)),
            )
            .add_systems(
                OnEnter(ClientState::Connected),
                receive_replication.in_set(ClientSystems::Receive),
            )
            .add_systems(
                OnExit(ClientState::Connected),
                reset.in_set(ClientSystems::Reset),
            );

        let auth_method = *app.world().resource::<AuthMethod>();
        debug!("using authorization method `{auth_method:?}`");
        if auth_method == AuthMethod::ProtocolCheck {
            app.add_observer(log_protocol_error).add_systems(
                OnEnter(ClientState::Connected),
                send_protocol_hash.in_set(ClientSystems::SendHash),
            );
        }

        if log_enabled!(Level::Debug) {
            app.add_systems(OnEnter(ClientState::Disconnected), || {
                debug!("disconnected")
            })
            .add_systems(OnEnter(ClientState::Connecting), || debug!("connecting"))
            .add_systems(OnEnter(ClientState::Connected), || debug!("connected"));
        }
    }

    fn finish(&self, app: &mut App) {
        app.world_mut()
            .resource_scope(|world, mut messages: Mut<ClientMessages>| {
                let channels = world.resource::<RepliconChannels>();
                messages.setup_server_channels(channels.server_channels().len());
            });
    }
}

/// Receives and applies replication messages from the server.
///
/// Update messages are sent over the [`ServerChannel::Updates`] and are applied first to ensure valid state
/// for component mutations.
///
/// Mutate messages are sent over [`ServerChannel::Mutations`], which means they may appear
/// ahead-of or behind update messages from the same server tick. A mutation will only be applied if its
/// update tick has already appeared in an update message, otherwise it will be buffered while waiting.
/// Since component mutations can arrive in any order, they will only be applied if they correspond to a more
/// recent server tick than the last acked server tick for each entity.
///
/// Buffered mutate messages are processed last.
///
/// Acknowledgments for received mutate messages are sent back to the server.
///
/// See also [`ReplicationMessages`](crate::server::replication_messages::ReplicationMessages).
pub(super) fn receive_replication(
    world: &mut World,
    mut scratch: Local<EntityScratch>,
    mut entity_markers: Local<EntityMarkers>,
    mut entity_buffer: Local<EntityBuffer>,
) {
    // Too many nested `resource_scope` break rustfmt.
    // Relevant issue to support multiple resources in a single scope: https://github.com/bevyengine/bevy/issues/23476
    let mut messages = world.remove_resource::<ClientMessages>().unwrap();
    let mut entity_map = world.remove_resource::<ServerEntityMap>().unwrap();
    let mut signature_map = world.remove_resource::<SignatureMap>().unwrap();
    let mut storage = world.remove_resource::<ReplicationStorage>().unwrap();
    let mut mutate_ticks = world.remove_resource::<ServerMutateTicks>().unwrap();
    let mut buffered_mutations = world.remove_resource::<BufferedMutations>().unwrap();
    let receive_markers = world.remove_resource::<ReceiveMarkers>().unwrap();
    let registry = world.remove_resource::<ReplicationRegistry>().unwrap();
    let mut replicated = world
        .remove_resource::<Messages<EntityReplicated>>()
        .unwrap();

    let type_registry = world.resource::<AppTypeRegistry>().clone();
    let mut stats = world.remove_resource::<ClientReplicationStats>();

    let mut params = ReceiveParams {
        scratch: &mut scratch,
        entity_markers: &mut entity_markers,
        entity_buffer: &mut entity_buffer,
        entity_map: &mut entity_map,
        signature_map: &mut signature_map,
        storage: &mut storage,
        mutate_ticks: &mut mutate_ticks,
        replicated: &mut replicated,
        stats: stats.as_mut(),
        receive_markers: &receive_markers,
        registry: &registry,
        type_registry: &type_registry,
    };

    apply_replication(world, &mut params, &mut messages, &mut buffered_mutations);

    if let Some(stats) = stats {
        world.insert_resource(stats);
    }

    world.insert_resource(messages);
    world.insert_resource(entity_map);
    world.insert_resource(signature_map);
    world.insert_resource(storage);
    world.insert_resource(mutate_ticks);
    world.insert_resource(buffered_mutations);
    world.insert_resource(receive_markers);
    world.insert_resource(registry);
    world.insert_resource(replicated);
}

// The storage resource may be unavailable while receiving replication.
// Cleanup is handled manually in the receive logic.
fn cleanup_storage(remove: On<Remove, Remote>, mut storage: If<ResMut<ReplicationStorage>>) {
    storage.entities.remove(&remove.entity);
}

// The server can despawn an entity without sending a replication message,
// so we need to manually remove the entity from the `ServerEntityMap`
// when it is despawned on the client.
fn cleanup_entity_map(despawn: On<Despawn, Remote>, mut entity_map: If<ResMut<ServerEntityMap>>) {
    entity_map.remove_by_client(despawn.entity);
}

fn reset(
    mut messages: ResMut<ClientMessages>,
    mut stats: ResMut<ClientStats>,
    mut update_tick: ResMut<ServerUpdateTick>,
    mut entity_map: ResMut<ServerEntityMap>,
    mut buffered_mutations: ResMut<BufferedMutations>,
    mutate_ticks: Option<ResMut<ServerMutateTicks>>,
    replication_stats: Option<ResMut<ClientReplicationStats>>,
) {
    messages.clear();
    *stats = Default::default();
    *update_tick = Default::default();
    entity_map.clear();
    buffered_mutations.clear();
    if let Some(mut mutate_ticks) = mutate_ticks {
        mutate_ticks.clear();
    }
    if let Some(mut replication_stats) = replication_stats {
        *replication_stats = Default::default();
    }
}

fn report_receive_error(world: &mut World, message: String) {
    error!("{message}");
    world.write_message(ClientReplicationError { message });
}

fn send_protocol_hash(mut commands: Commands, protocol: Res<ProtocolHash>) {
    debug!("sending `{:?}` to the server", *protocol);
    commands.client_trigger(*protocol);
}

fn log_protocol_error(_on: On<ProtocolMismatch>) {
    error!(
        "server reported protocol mismatch; make sure replication rules and events registration order match with the server"
    );
}

/// Reads all received messages and applies them.
///
/// Sends acknowledgments for mutate messages back.
fn apply_replication(
    world: &mut World,
    params: &mut ReceiveParams,
    messages: &mut ClientMessages,
    buffered_mutations: &mut BufferedMutations,
) {
    for mut message in messages.drain_received(ServerChannel::Updates) {
        if let Err(e) = apply_update_message(world, params, &mut message) {
            report_receive_error(world, format!("unable to apply update message: {e}"));

            // SAFETY: components in the scratch were pushed using this world.
            unsafe { params.scratch.manual_drop(world.components()) };
            params.entity_buffer.free(world);
        }
    }

    // Unlike update messages, we read all mutate messages first, sort them by tick
    // in descending order to ensure that the last mutation will be applied first.
    // Since mutate messages manually split by packet size, we apply all messages,
    // but skip outdated data per-entity by checking last received tick for it
    // (unless user requested history via marker).
    let update_tick = *world.resource::<ServerUpdateTick>();
    let mutations_count = messages.received_count(ServerChannel::Mutations);
    if mutations_count != 0 {
        let mut acks = Vec::with_capacity(MutateIndex::POSTCARD_MAX_SIZE * mutations_count);
        for message in messages.drain_received(ServerChannel::Mutations) {
            if let Err(e) = buffer_mutate_message(params, buffered_mutations, message, &mut acks) {
                report_receive_error(world, format!("unable to buffer mutate message: {e}"));
            }
        }
        messages.send(ClientChannel::MutationAcks, acks);
    }

    buffered_mutations.0.retain_mut(|mutate| {
        if mutate.update_tick.is_newer(*update_tick) {
            return true;
        }

        if let Err(e) = apply_mutate_message(world, params, mutate) {
            report_receive_error(
                world,
                format!(
                    "unable to apply mutate message for tick `{:?}`: {e}",
                    mutate.message_tick
                ),
            );

            // SAFETY: components in the scratch were pushed using this world.
            unsafe { params.scratch.manual_drop(world.components()) };
            params.entity_buffer.free(world);
        }

        false
    });
}

/// Reads and applies an update message.
///
/// For details see [`replication_messages`](crate::server::replication_messages).
fn apply_update_message(
    world: &mut World,
    params: &mut ReceiveParams,
    message: &mut Bytes,
) -> Result<()> {
    if let Some(stats) = &mut params.stats {
        stats.messages += 1;
        stats.bytes += message.len();
    }

    let flags: UpdateFlags = postcard_utils::from_buf(message)?;
    let message_tick = postcard_utils::from_buf(message)?;
    trace!("applying update message with `{flags:?}` for {message_tick:?}");
    world.resource_mut::<ServerUpdateTick>().0 = message_tick;

    let last_flag = flags.last();
    for (_, flag) in flags.iter_names() {
        let array_kind = if flag != last_flag {
            ArrayKind::Sized
        } else {
            ArrayKind::Dynamic
        };

        match flag {
            UpdateFlags::USERDATA => {
                process_userdata(world, message, message_tick)
                    .map_err(|e| format!("unable to process userdata: {e}"))?;
            }
            UpdateFlags::MAPPINGS => {
                let len = apply_array(array_kind, message, |message| {
                    apply_entity_mapping(world, params, message)
                })
                .map_err(|e| format!("unable to apply mappings: {e}"))?;
                if let Some(stats) = &mut params.stats {
                    stats.mappings += len;
                }
            }
            UpdateFlags::DESPAWNS => {
                let len = apply_array(array_kind, message, |message| {
                    apply_despawn(world, params, message, message_tick)
                })
                .map_err(|e| format!("unable to apply despawns: {e}"))?;
                if let Some(stats) = &mut params.stats {
                    stats.despawns += len;
                }
            }
            UpdateFlags::REMOVALS => {
                let len = apply_array(array_kind, message, |message| {
                    apply_removals(world, params, message, message_tick)
                })
                .map_err(|e| format!("unable to apply removals: {e}"))?;
                if let Some(stats) = &mut params.stats {
                    stats.entities_changed += len;
                }
            }
            UpdateFlags::CHANGES => {
                debug_assert_eq!(array_kind, ArrayKind::Dynamic);
                let len = apply_array(array_kind, message, |message| {
                    apply_changes(world, params, message, message_tick)
                })
                .map_err(|e| format!("unable to apply changes: {e}"))?;
                if let Some(stats) = &mut params.stats {
                    stats.entities_changed += len;
                }
            }
            _ => unreachable!("iteration should yield only named flags"),
        }
    }

    Ok(())
}

/// Reads and buffers mutate message.
///
/// For details see [`replication_messages`](crate::server::replication_messages).
///
/// Returns mutate index to be used for acknowledgment.
fn buffer_mutate_message(
    params: &mut ReceiveParams,
    buffered_mutations: &mut BufferedMutations,
    mut message: Bytes,
    acks: &mut Vec<u8>,
) -> Result<()> {
    if let Some(stats) = &mut params.stats {
        stats.messages += 1;
        stats.bytes += message.len();
    }

    let flags: MutateFlags = postcard_utils::from_buf(&mut message)?;
    let mutate_index: MutateIndex = postcard_utils::from_buf(&mut message)?;
    postcard_utils::to_extend_mut(&mutate_index, acks)?;

    let update_tick = postcard_utils::from_buf(&mut message)?;
    let message_tick = postcard_utils::from_buf(&mut message)?;
    trace!("received mutate message for {message_tick:?}");
    buffered_mutations.insert(BufferedMutate {
        flags,
        update_tick,
        message_tick,
        message,
    });

    Ok(())
}

/// Reads and applies a buffered mutate message.
fn apply_mutate_message(
    world: &mut World,
    params: &mut ReceiveParams,
    mutate: &mut BufferedMutate,
) -> Result<()> {
    trace!(
        "applying mutate message with `{:?}` for {:?}",
        mutate.flags, mutate.message_tick
    );

    for (_, flag) in mutate.flags.iter_names() {
        match flag {
            MutateFlags::USERDATA => {
                process_userdata(world, &mut mutate.message, mutate.message_tick)
                    .map_err(|e| format!("unable to apply userdata: {e}"))?;
            }
            MutateFlags::MESSAGES_COUNT => {
                confirm_mutate_tick(world, params.mutate_ticks, mutate)
                    .map_err(|e| format!("unable to confirm mutate tick: {e}"))?;
            }
            MutateFlags::MUTATIONS => {
                let len = apply_array(ArrayKind::Dynamic, &mut mutate.message, |message| {
                    apply_mutations(world, params, message, mutate.message_tick)
                })
                .map_err(|e| format!("unable to apply mutations: {e}"))?;
                if let Some(stats) = &mut params.stats {
                    stats.entities_changed += len;
                }
            }
            _ => unreachable!("iteration should yield only named flags"),
        }
    }

    Ok(())
}

/// Deserializes and applies the mapping from a server entity to a client
/// entity by comparing hashes calculated from the [`Signature`] component.
fn apply_entity_mapping(
    world: &mut World,
    params: &mut ReceiveParams,
    message: &mut Bytes,
) -> Result<()> {
    let server_entity = postcard_utils::entity_from_buf(message)?;
    let hash = u64::from_le_bytes(postcard_utils::from_buf(message)?); // Hash uses fixint encoding.

    let Some(client_entity) = params.signature_map.get(hash) else {
        // Client entity may have been despawned already.
        debug!("skipping unknown hash 0x{hash:016x} for `{server_entity}`");
        return Ok(());
    };

    debug!("mapping `{server_entity}` to `{client_entity}` using hash 0x{hash:016x}");
    params.entity_map.insert(server_entity, client_entity);
    world.entity_mut(client_entity).insert(Remote);

    Ok(())
}

/// Deserializes and applies entity despawn from update message.
fn apply_despawn(
    world: &mut World,
    params: &mut ReceiveParams,
    message: &mut Bytes,
    message_tick: RepliconTick,
) -> Result<()> {
    // The entity might have already been despawned because of hierarchy or
    // with the last replication message, but the server might not yet have received confirmation
    // from the client and could include the deletion in the this message.
    let server_entity = postcard_utils::entity_from_buf(message)?;
    if let Some(&client_entity) = params.entity_map.to_client().get(&server_entity) {
        let mut children = world.query::<&Children>();
        let subtree = core::iter::once(client_entity)
            .chain(
                children
                    .query(world)
                    .iter_descendants::<Children>(client_entity),
            )
            .collect::<Vec<_>>();

        if let Ok(client_entity) = world.get_entity_mut(client_entity) {
            debug!("applying despawn for `{}`", client_entity.id());
            let ctx = DespawnCtx { message_tick };
            (params.registry.despawn)(&ctx, client_entity);
        }

        for entity in subtree {
            if !world.entities().contains(entity) || world.get::<Remote>(entity).is_none() {
                // Requires manual removal since these resources are removed from the world and inaccessible to observers.
                let _ = params.entity_map.remove_by_client(entity);
                params.signature_map.remove(entity);
                params.storage.entities.remove(&entity);
            }
        }
    }

    Ok(())
}

/// Deserializes and applies component removals for an entity.
fn apply_removals(
    world: &mut World,
    params: &mut ReceiveParams,
    message: &mut Bytes,
    message_tick: RepliconTick,
) -> Result<()> {
    let server_entity = postcard_utils::entity_from_buf(message)?;
    let data_size: usize = postcard_utils::from_buf(message)?;

    let Some(&client_entity) = params.entity_map.to_client().get(&server_entity) else {
        // Client could predict despawn.
        debug!("ignoring removals for unknown server's `{server_entity}`");
        message.advance(data_size);
        return Ok(());
    };

    let Ok(mut client_entity) = world
        .get_entity_mut(client_entity)
        .map(|entity| DeferredEntity::new(entity, params.scratch))
    else {
        // Entity could've been disabled while despawned, which doesn't remove it from the entity mapping.
        debug!("ignoring removals for invalid `{client_entity}`");
        params.entity_map.remove_by_client(client_entity);
        message.advance(data_size);
        return Ok(());
    };

    params
        .entity_markers
        .read(params.receive_markers, &*client_entity);

    confirm_tick(&mut client_entity, params.replicated, message_tick);

    let mut data = message.split_to(data_size);
    let len = apply_array(ArrayKind::Dynamic, &mut data, |data| {
        let fns_id = postcard_utils::from_buf(data)?;
        let (_, component_id, fns) = params.registry.get(fns_id);
        let mut ctx = RemoveCtx {
            message_tick,
            component_id,
        };
        trace!(
            "applying removal for `{}` with `{fns_id:?}`",
            client_entity.id()
        );

        fns.remove(&mut ctx, params.entity_markers, &mut client_entity);

        Ok(())
    })?;

    if let Some(stats) = &mut params.stats {
        stats.components_changed += len;
    }

    client_entity.flush();

    Ok(())
}

/// Deserializes and applies component insertions and/or mutations for an entity.
fn apply_changes(
    world: &mut World,
    params: &mut ReceiveParams,
    message: &mut Bytes,
    message_tick: RepliconTick,
) -> Result<()> {
    debug_assert!(
        params.entity_buffer.is_empty(),
        "`{:?}` should be freed before reuse",
        params.entity_buffer
    );

    let server_entity = postcard_utils::entity_from_buf(message)?;
    let data_size: usize = postcard_utils::from_buf(message)?;

    let world_cell = world.as_unsafe_world_cell();
    let entity_allocator = world_cell.entity_allocator();
    // SAFETY: used only to create `DeferredEntity`, which won't let mutably alias `EntityAllocator`.
    let world = unsafe { world_cell.world_mut() };

    let client_entity = params
        .entity_map
        .get_or_insert_with(server_entity, || world.spawn_empty().id());
    let Ok(client_entity) = world.get_entity_mut(client_entity) else {
        // Client could predict despawn.
        debug!("ignoring changes for despawned `{client_entity}`");
        message.advance(data_size);
        return Ok(());
    };
    let mut client_entity = DeferredEntity::new(client_entity, params.scratch);
    if !client_entity.contains::<Remote>() {
        // Even though the entity already exists, it could have been spawned during
        // deserialization of another component and doesn't have the marker yet.
        client_entity.insert(Remote);
    }

    params
        .entity_markers
        .read(params.receive_markers, &*client_entity);

    confirm_tick(&mut client_entity, params.replicated, message_tick);

    let mut data = message.split_to(data_size);
    // The server sorts incoming markers with `affects_same_update` from highest to lowest priority
    // before all other components. Only the first component needs to be checked because, if it's a
    // marker, lower-priority markers can't affect receive-function selection.
    let mut first_component = true;
    let len = apply_array(ArrayKind::Dynamic, &mut data, |data| {
        let spawner = BufferedSpawner::new(entity_allocator, params.entity_buffer);
        let fns_id = postcard_utils::from_buf(data)?;
        let (_, component_id, fns) = params.registry.get(fns_id);
        if first_component {
            params
                .entity_markers
                .include(params.receive_markers, component_id);
            first_component = false;
        }
        let mut ctx = WriteCtx {
            entity: client_entity.id(),
            component_id,
            message_tick,
            entity_map: params.entity_map,
            storage: params.storage,
            type_registry: params.type_registry,
            spawner,
            ignore_mapping: false,
        };
        trace!(
            "applying change for `{}` with `{fns_id:?}`",
            client_entity.id(),
        );

        fns.write(&mut ctx, params.entity_markers, &mut client_entity, data)?;

        Ok(())
    })?;

    if let Some(stats) = &mut params.stats {
        stats.components_changed += len;
    }

    // SAFETY: only used to spawn entities.
    params
        .entity_buffer
        .spawn(unsafe { client_entity.world_mut() });
    client_entity.flush();

    Ok(())
}

fn process_userdata(
    world: &mut World,
    message: &mut Bytes,
    message_tick: RepliconTick,
) -> Result<()> {
    let len = postcard_utils::from_buf(message)?;
    if len > message.len() {
        return Err(format!(
            "userdata length ({len}) exceeds remaining message length ({})",
            message.len()
        )
        .into());
    }
    world.trigger(UserdataReceived {
        message_tick,
        bytes: message.split_to(len),
    });

    Ok(())
}

fn apply_array(
    kind: ArrayKind,
    message: &mut Bytes,
    mut f: impl FnMut(&mut Bytes) -> Result<()>,
) -> Result<usize> {
    match kind {
        ArrayKind::Sized => {
            let len = postcard_utils::from_buf(message)?;
            for _ in 0..len {
                (f)(message)?;
            }

            Ok(len)
        }
        ArrayKind::Dynamic => {
            let mut len = 0;
            while message.has_remaining() {
                (f)(message)?;
                len += 1;
            }

            Ok(len)
        }
    }
}

/// Type of serialized array.
#[derive(PartialEq, Eq, Debug)]
enum ArrayKind {
    /// Size is serialized before the array.
    Sized,
    /// Size is unknown, means that all bytes needs to be consumed.
    Dynamic,
}

fn confirm_tick(
    entity: &mut DeferredEntity,
    replicated: &mut Messages<EntityReplicated>,
    tick: RepliconTick,
) {
    if let Some(mut history) = entity.get_mut::<ConfirmHistory>() {
        history.set_last_tick(tick);
    } else {
        entity.insert(ConfirmHistory::new(tick));
    }
    replicated.write(EntityReplicated {
        entity: entity.id(),
        tick,
    });
}

fn confirm_mutate_tick(
    world: &mut World,
    mutate_ticks: &mut ServerMutateTicks,
    mutate: &mut BufferedMutate,
) -> Result<()> {
    let count = postcard_utils::from_buf(&mut mutate.message)?;
    if mutate_ticks.confirm(mutate.message_tick, count) {
        mutate_ticks.set_last_confirmed_tick(mutate.message_tick);
        world.write_message(MutateTickReceived {
            tick: mutate.message_tick,
        });
    }

    Ok(())
}

/// Deserializes and applies component mutations for an entity.
fn apply_mutations(
    world: &mut World,
    params: &mut ReceiveParams,
    message: &mut Bytes,
    message_tick: RepliconTick,
) -> Result<()> {
    debug_assert!(
        params.entity_buffer.is_empty(),
        "`{:?}` should be freed before applying mutations",
        params.entity_buffer
    );

    let server_entity = postcard_utils::entity_from_buf(message)?;
    let data_size: usize = postcard_utils::from_buf(message)?;

    let Some(&client_entity) = params.entity_map.to_client().get(&server_entity) else {
        // Mutation could arrive after a despawn from update message
        // or client could predict the despawn.
        debug!("ignoring mutations for unknown server's `{server_entity}`");
        message.advance(data_size);
        return Ok(());
    };

    let world_cell = world.as_unsafe_world_cell();
    let entity_allocator = world_cell.entity_allocator();
    // SAFETY: used only to create `DeferredEntity`, which won't let mutably alias `EntityAllocator`.
    let world = unsafe { world_cell.world_mut() };

    let Ok(mut client_entity) = world
        .get_entity_mut(client_entity)
        .map(|entity| DeferredEntity::new(entity, params.scratch))
    else {
        // Entity could've been disabled while despawned, which doesn't remove it from the entity mapping.
        debug!("ignoring mutations for invalid `{client_entity}`");
        params.entity_map.remove_by_client(client_entity);
        message.advance(data_size);
        return Ok(());
    };

    params
        .entity_markers
        .read(params.receive_markers, &*client_entity);

    let new_tick = if let Some(mut history) = client_entity.get_mut::<ConfirmHistory>() {
        let new_tick = message_tick.is_newer(history.last_tick());
        if new_tick {
            history.set_last_tick(message_tick);
        } else {
            if !params.entity_markers.need_history() {
                trace!("ignoring outdated mutations for `{}`", client_entity.id());
                message.advance(data_size);
                return Ok(());
            }

            let ago = history.last_tick().get().wrapping_sub(message_tick.get());
            if ago >= u64::BITS {
                trace!(
                    "discarding {ago} ticks old mutations for `{}`",
                    client_entity.id()
                );
                message.advance(data_size);
                return Ok(());
            }

            history.set(ago);
        }
        new_tick
    } else {
        client_entity.insert(ConfirmHistory::new(message_tick));
        true
    };
    params.replicated.write(EntityReplicated {
        entity: client_entity.id(),
        tick: message_tick,
    });

    let mut data = message.split_to(data_size);
    let len = apply_array(ArrayKind::Dynamic, &mut data, |data| {
        let spawner = BufferedSpawner::new(entity_allocator, params.entity_buffer);
        let fns_id = postcard_utils::from_buf(data)?;
        let (_, component_id, fns) = params.registry.get(fns_id);
        let mut ctx = WriteCtx {
            entity: client_entity.id(),
            component_id,
            message_tick,
            entity_map: params.entity_map,
            storage: params.storage,
            type_registry: params.type_registry,
            spawner,
            ignore_mapping: false,
        };
        trace!(
            "applying mutation for `{}` with `{fns_id:?}`",
            client_entity.id(),
        );

        if new_tick {
            fns.write(&mut ctx, params.entity_markers, &mut client_entity, data)?;
        } else {
            fns.consume_or_write(
                &mut ctx,
                params.entity_markers,
                params.receive_markers,
                &mut client_entity,
                data,
            )?;
        }

        Ok(())
    })?;

    if let Some(stats) = &mut params.stats {
        stats.components_changed += len;
    }

    // SAFETY: only used to spawn entities.
    params
        .entity_buffer
        .spawn(unsafe { client_entity.world_mut() });
    client_entity.flush();

    Ok(())
}

/// Borrowed resources from the world and locals.
///
/// To avoid passing a lot of arguments into all receive functions.
struct ReceiveParams<'a> {
    scratch: &'a mut EntityScratch,
    entity_markers: &'a mut EntityMarkers,
    entity_buffer: &'a mut EntityBuffer,
    entity_map: &'a mut ServerEntityMap,
    signature_map: &'a mut SignatureMap,
    storage: &'a mut ReplicationStorage,
    mutate_ticks: &'a mut ServerMutateTicks,
    replicated: &'a mut Messages<EntityReplicated>,
    stats: Option<&'a mut ClientReplicationStats>,
    receive_markers: &'a ReceiveMarkers,
    registry: &'a ReplicationRegistry,
    type_registry: &'a AppTypeRegistry,
}

/// Set with replication and event systems related to client.
#[derive(SystemSet, Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum ClientSystems {
    /// Systems that receive packets from the messaging backend and update [`ClientState`].
    ///
    /// Used by messaging backend implementations.
    ///
    /// Runs in [`PreUpdate`].
    ReceivePackets,
    /// Systems that read data from [`ClientMessages`].
    ///
    /// Runs in [`PreUpdate`] and [`OnEnter`] for [`ClientState::Connected`] (to avoid 1 frame delay).
    Receive,
    /// Systems that populate Bevy's [`Diagnostics`](bevy::diagnostic::Diagnostics).
    ///
    /// Runs in [`PreUpdate`] and [`OnEnter`] for [`ClientState::Connected`] (to avoid 1 frame delay).
    Diagnostics,
    /// System that sends [`ProtocolHash`].
    ///
    /// Runs in [`OnEnter`] for [`ClientState::Connected`].
    SendHash,
    /// Systems that write data to [`ClientMessages`].
    ///
    /// Runs in [`PostUpdate`].
    Send,
    /// Systems that send packets to the messaging backend.
    ///
    /// Used by messaging backend implementations.
    ///
    /// Runs in [`PostUpdate`].
    SendPackets,
    /// Systems that reset the client.
    ///
    /// Runs in [`OnExit`] for [`ClientState::Connected`].
    Reset,
}

/// Last received tick for update messages from the server.
///
/// In other words, the last [`RepliconTick`] with a removal, insertion, spawn or despawn.
/// This value is not updated when mutation messages are received from the server.
///
/// See also [`ServerMutateTicks`].
#[derive(Resource, Deref, Default, Reflect, Debug, Clone, Copy)]
pub struct ServerUpdateTick(RepliconTick);

/// Cached buffered mutate messages, used to synchronize mutations with update messages.
#[derive(Resource, Default)]
pub(crate) struct BufferedMutations(Vec<BufferedMutate>);

impl BufferedMutations {
    fn clear(&mut self) {
        self.0.clear();
    }

    /// Inserts a new buffered message, maintaining sorting by their message tick in descending order.
    fn insert(&mut self, mutate: BufferedMutate) {
        let index = self
            .0
            .partition_point(|other| mutate.message_tick.is_older(other.message_tick));
        self.0.insert(index, mutate);
    }
}

/// Partially-deserialized mutate message that is waiting for its tick to appear in an update message.
///
/// See also [`crate::server::replication_messages`].
pub(super) struct BufferedMutate {
    /// Flags for data in the message.
    flags: MutateFlags,

    /// Required tick to wait for.
    update_tick: RepliconTick,

    /// The tick this mutations corresponds to.
    message_tick: RepliconTick,

    /// Mutations data.
    message: Bytes,
}

/// Replication stats during message processing.
///
/// Statistic will be collected only if the resource is present.
/// The resource is not added by default.
///
/// See also [`ClientDiagnosticsPlugin`]
/// for automatic integration with Bevy diagnostics.
#[derive(Resource, Default, Reflect, Debug, Clone, Copy)]
pub struct ClientReplicationStats {
    /// Incremented per entity that changes.
    pub entities_changed: usize,
    /// Incremented for every component that changes.
    pub components_changed: usize,
    /// Incremented per client mapping added.
    pub mappings: usize,
    /// Incremented per entity despawn.
    pub despawns: usize,
    /// Replication messages received.
    pub messages: usize,
    /// Replication bytes received in message payloads (without internal messaging plugin data).
    pub bytes: usize,
}

/// An error occurred while decoding or applying a replication message.
#[derive(Message, Debug, Clone)]
pub struct ClientReplicationError {
    /// The underlying replication error.
    pub message: String,
}

/// Marker for entities spawned by replication.
///
/// Automatically inserted for each newly received entity.
///
/// Unlike [`Replicated`], it doesn't affect the replication
/// logic and exists purely as an informational marker.
/// For example, you can event register [`Replicated`] as required
/// component for it:
///
/// ```
/// # use bevy::prelude::*;
/// # use bevy_replicon::prelude::*;
/// # let mut app = App::new();
/// app.register_required_components::<Remote, Replicated>();
/// ```
///
/// This way, after a client disconnects, it can start a new server
/// with the same entities now being replicated.
#[derive(Component, Default, Reflect, Debug, Clone, Copy)]
#[reflect(Component)]
pub struct Remote;

#[cfg(test)]
mod tests {
    use bevy::state::app::StatesPlugin;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{
        postcard_utils,
        prelude::RepliconPlugins,
        shared::{
            backend::channels::ServerChannel,
            replication::registry::{
                ReplicationRegistry, rule_fns::RuleFns, test_fns::TestFnsEntityExt,
            },
        },
    };

    #[derive(Component)]
    struct DespawnIntercepted;

    #[derive(Component, Debug, Deserialize, PartialEq, Serialize)]
    struct MappedMutation(bool);

    fn preserve_despawn(_ctx: &DespawnCtx, mut entity: EntityWorldMut) {
        entity.insert(DespawnIntercepted);
    }

    fn remove_remote(_ctx: &DespawnCtx, mut entity: EntityWorldMut) {
        entity.remove::<Remote>();
    }

    fn queue_despawn(app: &mut App, server_entity: Entity, tick: RepliconTick) {
        let mut message = Vec::new();
        postcard_utils::to_extend_mut(&UpdateFlags::DESPAWNS, &mut message).unwrap();
        postcard_utils::to_extend_mut(&tick, &mut message).unwrap();
        postcard_utils::entity_to_extend_mut(&server_entity, &mut message).unwrap();
        app.world_mut()
            .resource_mut::<ClientMessages>()
            .insert_received(ServerChannel::Updates, message);
    }

    fn queue_mapping(app: &mut App, server_entity: Entity, hash: u64, tick: RepliconTick) {
        let mut message = Vec::new();
        postcard_utils::to_extend_mut(&UpdateFlags::MAPPINGS, &mut message).unwrap();
        postcard_utils::to_extend_mut(&tick, &mut message).unwrap();
        postcard_utils::entity_to_extend_mut(&server_entity, &mut message).unwrap();
        message.extend_from_slice(&hash.to_le_bytes());
        app.world_mut()
            .resource_mut::<ClientMessages>()
            .insert_received(ServerChannel::Updates, message);
    }

    fn queue_mutation(
        app: &mut App,
        server_entity: Entity,
        fns_id: crate::shared::replication::registry::FnsId,
        data: &[u8],
        tick: RepliconTick,
    ) {
        let mut mutation = Vec::new();
        postcard_utils::to_extend_mut(&fns_id, &mut mutation).unwrap();
        mutation.extend_from_slice(data);

        let mut message = Vec::new();
        postcard_utils::to_extend_mut(&MutateFlags::MUTATIONS, &mut message).unwrap();
        postcard_utils::to_extend_mut(&MutateIndex::default(), &mut message).unwrap();
        postcard_utils::to_extend_mut(&tick, &mut message).unwrap();
        postcard_utils::to_extend_mut(&tick, &mut message).unwrap();
        postcard_utils::entity_to_extend_mut(&server_entity, &mut message).unwrap();
        postcard_utils::to_extend_mut(&mutation.len(), &mut message).unwrap();
        message.extend_from_slice(&mutation);
        app.world_mut()
            .resource_mut::<ClientMessages>()
            .insert_received(ServerChannel::Mutations, message);
    }

    #[test]
    fn signature_mapping_does_not_confirm_replication() {
        let server_entity = Entity::from_raw_u32(100).unwrap();
        let hash = 42;
        let tick = RepliconTick::new(7);
        let mut app = App::new();
        app.add_plugins((MinimalPlugins, StatesPlugin, RepliconPlugins));
        app.finish();

        let client_entity = app.world_mut().spawn(Signature::from_hash(hash)).id();
        queue_mapping(&mut app, server_entity, hash, tick);
        app.world_mut()
            .resource_mut::<NextState<ClientState>>()
            .set(ClientState::Connected);
        app.update();

        let world = app.world();
        assert_eq!(
            world
                .resource::<ServerEntityMap>()
                .to_client()
                .get(&server_entity),
            Some(&client_entity)
        );
        assert!(world.entity(client_entity).contains::<Remote>());
        assert!(!world.entity(client_entity).contains::<ConfirmHistory>());
        assert!(world.resource::<Messages<EntityReplicated>>().is_empty());
    }

    #[test]
    fn first_same_tick_mutation_confirms_replication() {
        let server_entity = Entity::from_raw_u32(100).unwrap();
        let hash = 42;
        let tick = RepliconTick::new(7);
        let mut app = App::new();
        app.add_plugins((MinimalPlugins, StatesPlugin, RepliconPlugins));
        let (_, fns_id) =
            app.world_mut()
                .resource_scope(|world, mut registry: Mut<ReplicationRegistry>| {
                    registry.register_rule_fns(world, RuleFns::<MappedMutation>::default())
                });
        app.finish();

        let client_entity = app.world_mut().spawn(Signature::from_hash(hash)).id();
        let source = app.world_mut().spawn(MappedMutation(true)).id();
        let data = app.world_mut().entity_mut(source).serialize(fns_id, tick);
        app.world_mut().despawn(source);
        queue_mapping(&mut app, server_entity, hash, tick);
        queue_mutation(&mut app, server_entity, fns_id, &data, tick);
        app.world_mut()
            .resource_mut::<NextState<ClientState>>()
            .set(ClientState::Connected);
        app.update();

        let world = app.world();
        assert_eq!(
            world.entity(client_entity).get::<MappedMutation>(),
            Some(&MappedMutation(true))
        );
        let history = world.entity(client_entity).get::<ConfirmHistory>().unwrap();
        assert_eq!(history.last_tick(), tick);
        assert!(history.contains(tick));
        assert!(
            world
                .resource::<Messages<ClientReplicationError>>()
                .is_empty()
        );

        let mut replicated = app.world_mut().resource_mut::<Messages<EntityReplicated>>();
        let [replicated] = replicated.drain().collect::<Vec<_>>().try_into().unwrap();
        assert_eq!(replicated.entity, client_entity);
        assert_eq!(replicated.tick, tick);
    }

    #[test]
    fn custom_despawn_preserves_and_cleans_tracking() {
        let server_root = Entity::from_raw_u32(100).unwrap();
        let server_child = Entity::from_raw_u32(101).unwrap();
        let mut app = App::new();
        app.add_plugins((MinimalPlugins, StatesPlugin, RepliconPlugins));
        app.finish();

        let root = app
            .world_mut()
            .spawn((Remote, Signature::from_hash(1)))
            .id();
        let child = app
            .world_mut()
            .spawn((Remote, Signature::from_hash(2), ChildOf(root)))
            .id();
        {
            let world = app.world_mut();
            world
                .resource_mut::<ServerEntityMap>()
                .insert(server_root, root);
            world
                .resource_mut::<ServerEntityMap>()
                .insert(server_child, child);
            world
                .resource_mut::<ReplicationStorage>()
                .insert(root, 1_u32);
            world
                .resource_mut::<ReplicationStorage>()
                .insert(child, 2_u32);
            world.resource_mut::<ReplicationRegistry>().despawn = preserve_despawn;
        }

        queue_despawn(&mut app, server_root, RepliconTick::new(1));
        app.world_mut()
            .resource_mut::<NextState<ClientState>>()
            .set(ClientState::Connected);
        app.update();

        let world = app.world();
        assert!(world.entity(root).contains::<Remote>());
        assert!(world.entity(root).contains::<DespawnIntercepted>());
        assert!(
            world
                .resource::<ReplicationStorage>()
                .entities
                .contains_key(&root)
        );

        app.world_mut()
            .resource_mut::<ReplicationRegistry>()
            .despawn = remove_remote;
        queue_despawn(&mut app, server_root, RepliconTick::new(2));
        app.update();

        let world = app.world();
        assert!(!world.entity(root).contains::<Remote>());
        assert!(world.entity(child).contains::<Remote>());
        assert!(
            !world
                .resource::<ServerEntityMap>()
                .to_server()
                .contains_key(&root)
        );
        assert_eq!(
            world.resource::<ServerEntityMap>().to_server().get(&child),
            Some(&server_child)
        );
        assert!(world.resource::<SignatureMap>().get(1).is_none());
        assert_eq!(world.resource::<SignatureMap>().get(2), Some(child));
        assert!(
            !world
                .resource::<ReplicationStorage>()
                .entities
                .contains_key(&root)
        );
        assert!(
            world
                .resource::<ReplicationStorage>()
                .entities
                .contains_key(&child)
        );
    }
}
/// Triggered when user-defined bytes are received in a replication message.
///
/// This is emitted for data sent through [`ReplicationUserdata`](crate::server::ReplicationUserdata).
#[derive(Event)]
pub struct UserdataReceived {
    /// Tick of the message with the metadata.
    pub message_tick: RepliconTick,

    /// Raw user-defined bytes received from the server.
    pub bytes: Bytes,
}
