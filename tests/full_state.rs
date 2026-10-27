use bevy::{prelude::*, state::app::StatesPlugin};
use bevy_replicon::{
    prelude::*,
    server::ReplicateFullState,
    shared::{backend::channels::ServerChannel, server_entity_map::ServerEntityMap},
    test_app::ServerTestAppExt,
};
use serde::{Deserialize, Serialize};

#[derive(Component, Debug, PartialEq, Serialize, Deserialize)]
struct Health(u32);

#[derive(Component, Debug, PartialEq, Serialize, Deserialize)]
struct Position(u32);

#[derive(Component, Serialize, Deserialize)]
struct Prerequisite;

fn app() -> App {
    let mut app = App::new();
    app.add_plugins((
        MinimalPlugins,
        StatesPlugin,
        RepliconPlugins.set(ServerPlugin::new(PostUpdate)),
    ))
    .replicate::<Health>()
    .replicate_once::<Position>()
    .replicate_once::<Prerequisite>()
    .finish();
    app
}

#[test]
fn full_state_includes_acknowledged_but_unapplied_mutations_and_once_components() {
    let (mut server, mut client) = (app(), app());
    let entity = server
        .world_mut()
        .spawn((Replicated, Health(100), Position(0)))
        .id();
    server.connect_client(&mut client);
    server.update();
    server.exchange_with_client(&mut client);
    client.update();
    server.exchange_with_client(&mut client);
    let local = client.world().resource::<ServerEntityMap>().to_client()[&entity];

    server.world_mut().entity_mut(entity).insert(Prerequisite);
    server.update();
    let mut held = Vec::new();
    server
        .world_mut()
        .resource_mut::<ServerMessages>()
        .retain_sent(|(_, channel, bytes)| {
            if *channel == usize::from(ServerChannel::Updates) {
                held.push((*channel, bytes.clone()));
                false
            } else {
                true
            }
        });
    assert!(!held.is_empty());
    server.exchange_with_client(&mut client);
    client.update();
    server.exchange_with_client(&mut client);

    server
        .world_mut()
        .entity_mut(entity)
        .insert((Health(7), Position(42)));
    server.update();
    server.exchange_with_client(&mut client);
    client.update();
    server.exchange_with_client(&mut client);
    // Consume the receipt acknowledgement before requesting the full state.
    server.update();
    assert_eq!(client.world().get::<Health>(local), Some(&Health(100)));

    server
        .world_mut()
        .entity_mut(entity)
        .insert(ReplicateFullState);
    server.update();
    for (channel, bytes) in held {
        client
            .world_mut()
            .resource_mut::<ClientMessages>()
            .insert_received(channel, bytes);
    }
    server.exchange_with_client(&mut client);
    client.update();
    assert_eq!(client.world().get::<Health>(local), Some(&Health(7)));
    assert_eq!(client.world().get::<Position>(local), Some(&Position(42)));

    server.exchange_with_client(&mut client);
    server.update();
    assert!(
        server
            .world_mut()
            .resource_mut::<ServerMessages>()
            .drain_sent()
            .all(
                |(_, channel, _)| channel != usize::from(ServerChannel::Updates)
                    && channel != usize::from(ServerChannel::Mutations)
            )
    );

    server
        .world_mut()
        .entity_mut(entity)
        .insert((Position(84), ReplicateFullState));
    server.update();
    server.exchange_with_client(&mut client);
    client.update();
    assert_eq!(client.world().get::<Position>(local), Some(&Position(84)));
}
