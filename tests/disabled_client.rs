use bevy::{ecs::entity_disabling::Disabled, prelude::*, state::app::StatesPlugin};
use bevy_replicon::{
    prelude::*,
    server::ReplicateFullState,
    shared::{backend::channels::ServerChannel, server_entity_map::ServerEntityMap},
    test_app::{ServerTestAppExt, TestClientEntity},
};
use serde::{Deserialize, Serialize};

fn app() -> App {
    let mut app = App::new();
    app.add_plugins((
        MinimalPlugins,
        StatesPlugin,
        RepliconPlugins.set(ServerPlugin::new(PostUpdate)),
    ))
    .replicate::<Health>()
    .replicate_once::<Position>()
    .replicate::<Removable>()
    .add_visibility_filter::<Team>()
    .finish();
    app
}

fn tick(server: &mut App, clients: &mut [&mut App]) {
    server.update();
    for client in clients {
        server.exchange_with_client(client);
        client.update();
        server.exchange_with_client(client);
    }
}

fn local(client: &App, entity: Entity) -> Entity {
    client.world().resource::<ServerEntityMap>().to_client()[&entity]
}

#[test]
fn full_state_and_deferred_updates_catch_up_only_the_resumed_client() {
    let (mut server, mut paused, mut active) = (app(), app(), app());
    server.connect_client(&mut paused);
    server.connect_client(&mut active);
    let paused_id = **paused.world().resource::<TestClientEntity>();
    let active_id = **active.world().resource::<TestClientEntity>();
    for client in [paused_id, active_id] {
        server.world_mut().entity_mut(client).insert(Team(0));
    }

    let retained = server
        .world_mut()
        .spawn((Replicated, Health(100), Position(0), Removable))
        .id();
    let despawned = server.world_mut().spawn(Replicated).id();
    let private = server
        .world_mut()
        .spawn((Replicated, Health(404), Team(1)))
        .id();
    for _ in 0..4 {
        tick(&mut server, &mut [&mut paused, &mut active]);
    }
    let paused_retained = local(&paused, retained);
    let paused_despawned = local(&paused, despawned);
    let active_retained = local(&active, retained);
    let active_despawned = local(&active, despawned);

    server.world_mut().entity_mut(paused_id).insert(Disabled);
    server
        .world_mut()
        .entity_mut(retained)
        .remove::<Removable>()
        .insert((Health(7), Position(42), ReplicateFullState));
    server
        .world_mut()
        .entity_mut(private)
        .insert(ReplicateFullState);
    server.world_mut().despawn(despawned);
    let spawned = server.world_mut().spawn((Replicated, Health(21))).id();

    // Let the active client's ACKs settle and the archetype scan become idle.
    for _ in 0..4 {
        tick(&mut server, &mut [&mut paused, &mut active]);
    }
    assert_eq!(
        paused.world().get::<Health>(paused_retained),
        Some(&Health(100))
    );
    assert_eq!(
        paused.world().get::<Position>(paused_retained),
        Some(&Position(0))
    );
    assert!(paused.world().get::<Removable>(paused_retained).is_some());
    assert!(paused.world().get_entity(paused_despawned).is_ok());
    assert!(
        !paused
            .world()
            .resource::<ServerEntityMap>()
            .to_client()
            .contains_key(&spawned)
    );
    assert_eq!(
        active.world().get::<Health>(active_retained),
        Some(&Health(7))
    );
    assert_eq!(
        active.world().get::<Position>(active_retained),
        Some(&Position(42))
    );
    assert!(active.world().get::<Removable>(active_retained).is_none());
    assert!(active.world().get_entity(active_despawned).is_err());

    server
        .world_mut()
        .entity_mut(paused_id)
        .remove::<Disabled>();
    tick(&mut server, &mut [&mut paused, &mut active]);
    assert_eq!(
        paused.world().get::<Health>(paused_retained),
        Some(&Health(7))
    );
    assert_eq!(
        paused.world().get::<Position>(paused_retained),
        Some(&Position(42))
    );
    assert!(paused.world().get::<Removable>(paused_retained).is_none());
    assert!(paused.world().get_entity(paused_despawned).is_err());
    assert_eq!(
        paused.world().get::<Health>(local(&paused, spawned)),
        Some(&Health(21))
    );
    for client in [&paused, &active] {
        assert!(
            !client
                .world()
                .resource::<ServerEntityMap>()
                .to_client()
                .contains_key(&private)
        );
    }

    // Re-enabling must drain the queue, not resend catch-up on every tick.
    for _ in 0..4 {
        tick(&mut server, &mut [&mut paused, &mut active]);
    }
    server.update();
    assert!(
        server
            .world_mut()
            .resource_mut::<ServerMessages>()
            .drain_sent()
            .next()
            .is_none()
    );
}

#[test]
fn removal_and_reinsertion_are_reliable_and_atomic_after_resume() {
    let (mut server, mut paused, mut active) = (app(), app(), app());
    server.connect_client(&mut paused);
    server.connect_client(&mut active);
    let paused_id = **paused.world().resource::<TestClientEntity>();
    let entity = server
        .world_mut()
        .spawn((Replicated, Health(100), Position(0)))
        .id();
    for _ in 0..4 {
        tick(&mut server, &mut [&mut paused, &mut active]);
    }
    let paused_entity = local(&paused, entity);

    server.world_mut().entity_mut(paused_id).insert(Disabled);
    server.world_mut().entity_mut(entity).remove::<Position>();
    tick(&mut server, &mut [&mut paused, &mut active]);
    server
        .world_mut()
        .entity_mut(entity)
        .insert((Health(7), Position(42)));
    for _ in 0..4 {
        tick(&mut server, &mut [&mut paused, &mut active]);
    }
    assert_eq!(
        paused.world().get::<Position>(paused_entity),
        Some(&Position(0))
    );

    server
        .world_mut()
        .entity_mut(paused_id)
        .remove::<Disabled>();
    server.update();
    let mut channels = Vec::new();
    server
        .world_mut()
        .resource_mut::<ServerMessages>()
        .retain_sent(|(client, channel, _)| {
            if *client == paused_id {
                channels.push(*channel);
            }
            true
        });
    assert!(channels.contains(&usize::from(ServerChannel::Updates)));
    assert!(
        !channels.contains(&usize::from(ServerChannel::Mutations)),
        "mutations accompanying deferred removals must be reliable"
    );
    server.exchange_with_client(&mut paused);
    paused.update();
    assert_eq!(
        paused.world().get::<Health>(paused_entity),
        Some(&Health(7))
    );
    assert_eq!(
        paused.world().get::<Position>(paused_entity),
        Some(&Position(42))
    );
}

#[test]
fn in_flight_mutation_ack_is_processed_while_client_is_disabled() {
    let (mut server, mut client) = (app(), app());
    server.connect_client(&mut client);
    let client_id = **client.world().resource::<TestClientEntity>();
    let entity = server.world_mut().spawn((Replicated, Health(100))).id();
    for _ in 0..4 {
        tick(&mut server, &mut [&mut client]);
    }

    server.world_mut().entity_mut(entity).insert(Health(7));
    tick(&mut server, &mut [&mut client]);
    assert_eq!(
        client.world().get::<Health>(local(&client, entity)),
        Some(&Health(7))
    );
    // The ACK is now in the server's receive buffer, but hasn't been processed.
    server.world_mut().entity_mut(client_id).insert(Disabled);
    tick(&mut server, &mut [&mut client]);
    server
        .world_mut()
        .entity_mut(client_id)
        .remove::<Disabled>();
    server.update();
    assert!(
        server
            .world_mut()
            .resource_mut::<ServerMessages>()
            .drain_sent()
            .next()
            .is_none(),
        "the already acknowledged mutation must not be retransmitted after resume"
    );
}

#[test]
fn delayed_ack_does_not_discard_deferred_removals() {
    let (mut server, mut client) = (app(), app());
    server.connect_client(&mut client);
    let client_id = **client.world().resource::<TestClientEntity>();
    let entity = server
        .world_mut()
        .spawn((Replicated, Health(100), Position(0)))
        .id();
    for _ in 0..4 {
        tick(&mut server, &mut [&mut client]);
    }
    let client_entity = local(&client, entity);

    server.world_mut().entity_mut(entity).insert(Health(7));
    server.update();
    server.exchange_with_client(&mut client);
    client.update();
    let held_acks: Vec<_> = client
        .world_mut()
        .resource_mut::<ClientMessages>()
        .drain_sent()
        .collect();
    assert!(!held_acks.is_empty());
    server.world_mut().entity_mut(client_id).insert(Disabled);
    server.world_mut().entity_mut(entity).remove::<Position>();
    tick(&mut server, &mut [&mut client]);

    // Deliver the old mutation ACK only after the removal was deferred.
    for (channel, bytes) in held_acks {
        server
            .world_mut()
            .resource_mut::<ServerMessages>()
            .insert_received(client_id, channel, bytes);
    }
    tick(&mut server, &mut [&mut client]);
    server.world_mut().entity_mut(entity).insert(Health(21));
    server
        .world_mut()
        .entity_mut(client_id)
        .remove::<Disabled>();
    tick(&mut server, &mut [&mut client]);
    assert_eq!(
        client.world().get::<Health>(client_entity),
        Some(&Health(21))
    );
    assert!(client.world().get::<Position>(client_entity).is_none());
}

#[test]
fn visibility_changes_while_paused_preserve_each_viewers_scope() {
    let (mut server, mut paused, mut active) = (app(), app(), app());
    server.connect_client(&mut paused);
    server.connect_client(&mut active);
    let paused_id = **paused.world().resource::<TestClientEntity>();
    let active_id = **active.world().resource::<TestClientEntity>();
    for client in [paused_id, active_id] {
        server.world_mut().entity_mut(client).insert(Team(0));
    }
    let previous = server
        .world_mut()
        .spawn((Replicated, Team(0), Health(7)))
        .id();
    let next = server
        .world_mut()
        .spawn((Replicated, Team(1), Health(21)))
        .id();
    for _ in 0..4 {
        tick(&mut server, &mut [&mut paused, &mut active]);
    }
    let previous_paused = local(&paused, previous);
    let previous_active = local(&active, previous);

    server
        .world_mut()
        .entity_mut(paused_id)
        .insert((Disabled, Team(1)));
    server.world_mut().entity_mut(next).insert(Health(42));
    for _ in 0..4 {
        tick(&mut server, &mut [&mut paused, &mut active]);
    }
    assert!(paused.world().get_entity(previous_paused).is_ok());
    assert!(
        !paused
            .world()
            .resource::<ServerEntityMap>()
            .to_client()
            .contains_key(&next)
    );

    server
        .world_mut()
        .entity_mut(paused_id)
        .remove::<Disabled>();
    tick(&mut server, &mut [&mut paused, &mut active]);
    assert!(paused.world().get_entity(previous_paused).is_err());
    assert_eq!(
        paused.world().get::<Health>(local(&paused, next)),
        Some(&Health(42))
    );
    assert_eq!(
        active.world().get::<Health>(previous_active),
        Some(&Health(7))
    );
    assert!(
        !active
            .world()
            .resource::<ServerEntityMap>()
            .to_client()
            .contains_key(&next)
    );
}

#[derive(Component, Debug, PartialEq, Serialize, Deserialize)]
struct Health(u32);

#[derive(Component, Debug, PartialEq, Serialize, Deserialize)]
struct Position(u32);

#[derive(Component, Serialize, Deserialize)]
struct Removable;

#[derive(Component, PartialEq)]
#[component(immutable)]
struct Team(u8);

impl VisibilityFilter for Team {
    type ClientComponent = Self;
    type Scope = Entity;

    fn is_visible(&self, _client: Entity, component: Option<&Self>) -> bool {
        component == Some(self)
    }
}
