use test_log::test;

use bevy::{prelude::*, state::app::StatesPlugin};
use bevy_replicon::{
    prelude::*,
    shared::backend::channels::ServerChannel,
    test_app::{ServerTestAppExt, TestClientEntity},
};
use serde::{Deserialize, Serialize};

#[test]
fn client_priority() {
    let mut server_app = App::new();
    let mut client_app = App::new();
    for app in [&mut server_app, &mut client_app] {
        app.add_plugins((
            MinimalPlugins,
            StatesPlugin,
            RepliconPlugins.set(ServerPlugin::new(PostUpdate)),
        ))
        .replicate::<BoolComponent>()
        .finish();
    }

    server_app.connect_client(&mut client_app);

    let server_entity = server_app
        .world_mut()
        .spawn((Replicated, BoolComponent(false)))
        .id();

    let client = **client_app.world().resource::<TestClientEntity>();
    let mut priority = server_app
        .world_mut()
        .get_mut::<PriorityMap>(client)
        .unwrap();
    priority.insert(server_entity, 0.5);

    server_app.update();
    server_app.exchange_with_client(&mut client_app);
    client_app.update();
    server_app.exchange_with_client(&mut client_app);

    // Change value.
    let mut component = server_app
        .world_mut()
        .get_mut::<BoolComponent>(server_entity)
        .unwrap();
    component.0 = true;

    server_app.update();
    server_app.exchange_with_client(&mut client_app);
    client_app.update();
    server_app.exchange_with_client(&mut client_app);

    let mut components = client_app.world_mut().query::<&BoolComponent>();
    let component = components.single(client_app.world()).unwrap();
    assert!(!component.0, "mutation should be deprioritized");

    server_app.update();
    server_app.exchange_with_client(&mut client_app);
    client_app.update();

    let component = components.single(client_app.world()).unwrap();
    assert!(component.0);
}

#[test]
fn entity_priority() {
    let mut server_app = App::new();
    let mut client_app = App::new();
    for app in [&mut server_app, &mut client_app] {
        app.add_plugins((
            MinimalPlugins,
            StatesPlugin,
            RepliconPlugins.set(ServerPlugin::new(PostUpdate)),
        ))
        .replicate::<BoolComponent>()
        .finish();
    }

    server_app.connect_client(&mut client_app);

    let server_entity = server_app
        .world_mut()
        .spawn((Replicated, ReplicatePriority(0.5), BoolComponent(false)))
        .id();

    server_app.update();
    server_app.exchange_with_client(&mut client_app);
    client_app.update();
    server_app.exchange_with_client(&mut client_app);

    // Change value.
    let mut component = server_app
        .world_mut()
        .get_mut::<BoolComponent>(server_entity)
        .unwrap();
    component.0 = true;

    server_app.update();
    server_app.exchange_with_client(&mut client_app);
    client_app.update();
    server_app.exchange_with_client(&mut client_app);

    let mut components = client_app.world_mut().query::<&BoolComponent>();
    let component = components.single(client_app.world()).unwrap();
    assert!(!component.0, "mutation should be deprioritized");

    server_app.update();
    server_app.exchange_with_client(&mut client_app);
    client_app.update();

    let component = components.single(client_app.world()).unwrap();
    assert!(component.0);
}

#[test]
fn client_priority_overrides_entity_priority() {
    let mut server_app = App::new();
    let mut client_app = App::new();
    for app in [&mut server_app, &mut client_app] {
        app.add_plugins((
            MinimalPlugins,
            StatesPlugin,
            RepliconPlugins.set(ServerPlugin::new(PostUpdate)),
        ))
        .replicate::<BoolComponent>()
        .finish();
    }

    server_app.connect_client(&mut client_app);

    let server_entity = server_app
        .world_mut()
        .spawn((Replicated, ReplicatePriority(0.5), BoolComponent(false)))
        .id();

    let client = **client_app.world().resource::<TestClientEntity>();
    let mut priority = server_app
        .world_mut()
        .get_mut::<PriorityMap>(client)
        .unwrap();
    priority.insert(server_entity, 1.0);

    server_app.update();
    server_app.exchange_with_client(&mut client_app);
    client_app.update();
    server_app.exchange_with_client(&mut client_app);

    // Change value.
    let mut component = server_app
        .world_mut()
        .get_mut::<BoolComponent>(server_entity)
        .unwrap();
    component.0 = true;

    server_app.update();
    server_app.exchange_with_client(&mut client_app);
    client_app.update();

    let mut components = client_app.world_mut().query::<&BoolComponent>();
    let component = components.single(client_app.world()).unwrap();
    assert!(
        component.0,
        "client priority should override entity priority"
    );
}

#[test]
fn with_miss() {
    let mut server_app = App::new();
    let mut client_app = App::new();
    for app in [&mut server_app, &mut client_app] {
        app.add_plugins((
            MinimalPlugins,
            StatesPlugin,
            RepliconPlugins.set(ServerPlugin::new(PostUpdate)),
        ))
        .replicate::<BoolComponent>()
        .finish();
    }

    server_app.connect_client(&mut client_app);

    let server_entity = server_app
        .world_mut()
        .spawn((Replicated, BoolComponent(false)))
        .id();

    let client = **client_app.world().resource::<TestClientEntity>();
    let mut priority = server_app
        .world_mut()
        .get_mut::<PriorityMap>(client)
        .unwrap();
    priority.insert(server_entity, 0.5);

    server_app.update();
    server_app.exchange_with_client(&mut client_app);
    client_app.update();
    server_app.exchange_with_client(&mut client_app);

    // Change value.
    let mut component = server_app
        .world_mut()
        .get_mut::<BoolComponent>(server_entity)
        .unwrap();
    component.0 = true;

    server_app.update();
    server_app.exchange_with_client(&mut client_app);
    client_app.update();
    server_app.exchange_with_client(&mut client_app);

    let mut components = client_app.world_mut().query::<&BoolComponent>();
    let component = components.single(client_app.world()).unwrap();
    assert!(!component.0, "mutation should be deprioritized");

    server_app.update();

    // Take and drop the mutation message.
    let mut messages = server_app.world_mut().resource_mut::<ServerMessages>();
    assert_eq!(messages.drain_sent().len(), 1);

    server_app.exchange_with_client(&mut client_app);
    client_app.update();
    server_app.exchange_with_client(&mut client_app);

    let mut components = client_app.world_mut().query::<&BoolComponent>();
    let component = components.single(client_app.world()).unwrap();
    assert!(!component.0);

    server_app.update();
    server_app.exchange_with_client(&mut client_app);
    client_app.update();

    let component = components.single(client_app.world()).unwrap();
    assert!(component.0, "change should be resent");
}

#[test]
fn with_insertion() {
    assert_atomic_priority_change(false);
}

#[test]
fn with_removal() {
    assert_atomic_priority_change(true);
}

fn assert_atomic_priority_change(initially_present: bool) {
    let mut server_app = App::new();
    let mut client_app = App::new();
    for app in [&mut server_app, &mut client_app] {
        app.add_plugins((
            MinimalPlugins,
            StatesPlugin,
            RepliconPlugins.set(ServerPlugin::new(PostUpdate)),
        ))
        .replicate::<BoolComponent>()
        .replicate::<TestComponent>()
        .finish();
    }

    server_app.connect_client(&mut client_app);

    let mut server_entity = server_app
        .world_mut()
        .spawn((Replicated, BoolComponent(false)));
    if initially_present {
        server_entity.insert(TestComponent);
    }
    let server_entity = server_entity.id();

    let client = **client_app.world().resource::<TestClientEntity>();
    let mut priority = server_app
        .world_mut()
        .get_mut::<PriorityMap>(client)
        .unwrap();
    priority.insert(server_entity, 0.5);

    server_app.update();
    server_app.exchange_with_client(&mut client_app);
    client_app.update();
    server_app.exchange_with_client(&mut client_app);

    let mut server_entity = server_app.world_mut().entity_mut(server_entity);
    server_entity.get_mut::<BoolComponent>().unwrap().0 = true;
    if initially_present {
        server_entity.remove::<TestComponent>();
    } else {
        server_entity.insert(TestComponent);
    }

    server_app.update();
    server_app
        .world_mut()
        .resource_mut::<ServerMessages>()
        .retain_sent(|(_, channel, _)| *channel == usize::from(ServerChannel::Updates));
    server_app.exchange_with_client(&mut client_app);
    client_app.update();

    let (component, present) = client_app
        .world_mut()
        .query::<(&BoolComponent, Has<TestComponent>)>()
        .single(client_app.world())
        .unwrap();
    assert_eq!(present, !initially_present);
    assert!(
        component.0,
        "mutation should be sent with the structural change"
    );
}

#[derive(Clone, Component, Copy, Deserialize, Serialize)]
struct BoolComponent(bool);

#[derive(Component, Deserialize, Serialize)]
struct TestComponent;
