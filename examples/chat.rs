use bevy::MinimalPlugins;
use bevy::app::{App, FixedUpdate};
use bevy_app::{PluginGroup, ScheduleRunnerPlugin};
use bevy_ecs::message::PopulatedMessageReader;
use bevy_ecs::observer::On;
use bevy_ecs::resource::Resource;
use bevy_ecs::system::{Commands, Res};
use bevy_p2p::bitcode::{Decode, Encode};
use bevy_p2p::events::{Binded, ConnectFailed, PeerConnected, PeerDisconnected};
use bevy_p2p::iroh::EndpointId;
use bevy_p2p::iroh_res::{IrohBind, IrohConnect, IrohResource};
use bevy_p2p::message::{MessageReceived, Net};
use bevy_p2p::plugin::P2PPlugin;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write, stdin};
use std::str::FromStr;
use std::sync::mpsc::Receiver;
use std::sync::{Mutex, mpsc};
use std::thread;
use std::time::Duration;
#[derive(Resource)]
struct Lines {
    rx: Mutex<Receiver<String>>,
}
fn main() {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in stdin().lock().lines().flatten() {
            tx.send(line).unwrap();
        }
    });
    let mut app = App::new();
    app.add_plugins(
        MinimalPlugins.set(ScheduleRunnerPlugin::run_loop(Duration::from_secs_f64(0.1))),
    );
    app.add_plugins(P2PPlugin::<Msg>::new());
    app.world_mut().trigger(IrohBind);
    app.insert_resource(Lines { rx: Mutex::new(rx) });
    app.add_systems(FixedUpdate, (update, receive_message));
    app.add_observer(on_bind);
    app.add_observer(on_connect_failed);
    app.add_observer(on_connect);
    app.add_observer(on_disconnect);
    app.run();
}
fn on_connect_failed(event: On<ConnectFailed>) {
    println!("{} failed", event.peer.fmt_short());
}
fn on_connect(event: On<PeerConnected>) {
    println!("{} connect", event.peer.fmt_short());
}
fn on_disconnect(event: On<PeerDisconnected>) {
    println!("{} disconnect", event.peer.fmt_short());
}
fn on_bind(_: On<Binded>, mut commands: Commands, iroh: Res<IrohResource<Msg>>) {
    let mut file = OpenOptions::new()
        .append(true)
        .write(true)
        .read(true)
        .create(true)
        .truncate(false)
        .open("chats")
        .unwrap();
    for line in BufReader::new(&file).lines().flatten() {
        if let Ok(endpoint) = EndpointId::from_str(&line) {
            let peer = EndpointId::from(endpoint);
            commands.trigger(IrohConnect::new(peer));
        }
    }
    file.write_fmt(format_args!("{}\n", iroh.my_id)).unwrap();
    println!("{}", iroh.my_id.fmt_short());
}
fn update(net: Net<Msg>, rx: Res<Lines>) {
    if let Ok(line) = rx.rx.lock().unwrap().try_recv() {
        net.broadcast(Msg::Chat(line));
    }
}
fn receive_message(mut reader: PopulatedMessageReader<MessageReceived<Msg>>) {
    for msg in reader.read() {
        match &msg.message {
            Msg::Chat(str) => {
                println!("{}: {str}", msg.peer.fmt_short());
            }
        }
    }
}
#[derive(Encode, Decode)]
pub enum Msg {
    Chat(String),
}
