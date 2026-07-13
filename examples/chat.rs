use bevy::MinimalPlugins;
use bevy::app::{App, FixedUpdate};
use bevy_app::{FixedPostUpdate, Startup};
use bevy_ecs::message::PopulatedMessageReader;
use bevy_ecs::resource::Resource;
use bevy_ecs::system::{Commands, Res};
use bevy_p2p::bitcode::{Decode, Encode};
use bevy_p2p::iroh::EndpointId;
use bevy_p2p::iroh_res::{IrohBind, IrohConnect, IrohResource};
use bevy_p2p::message::{ConnectFailed, MessageReceived, Net, PeerConnected, PeerDisconnected};
use bevy_p2p::plugin::P2PPlugin;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write, stdin};
use std::str::FromStr;
use std::sync::mpsc::Receiver;
use std::sync::{Mutex, mpsc};
use std::thread;
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
    app.add_plugins(MinimalPlugins);
    app.add_plugins(P2PPlugin::<Msg>::new());
    app.world_mut().trigger(IrohBind);
    app.insert_resource(Lines { rx: Mutex::new(rx) });
    app.add_systems(Startup, startup);
    app.add_systems(
        FixedUpdate,
        (update, connect_failed, on_connect, receive_message),
    );
    app.add_systems(FixedPostUpdate, on_disconnect);
    app.run();
}
fn connect_failed(mut reader: PopulatedMessageReader<ConnectFailed>) {
    for peer in reader.read() {
        println!("{} failed", peer.peer.fmt_short());
    }
}
fn on_connect(mut reader: PopulatedMessageReader<PeerConnected>) {
    for peer in reader.read() {
        println!("{} connect", peer.peer.fmt_short());
    }
}
fn on_disconnect(mut reader: PopulatedMessageReader<PeerDisconnected>) {
    for peer in reader.read() {
        println!("{} disconnect", peer.peer.fmt_short());
    }
}
fn startup(mut commands: Commands, iroh: Res<IrohResource<Msg>>) {
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
    file.write_fmt(format_args!("{}\n", iroh.router.endpoint().id()))
        .unwrap();
    println!("{}", iroh.router.endpoint().id().fmt_short());
}
fn update(mut net: Net<Msg>, rx: Res<Lines>) {
    if let Ok(line) = rx.rx.lock().unwrap().try_recv() {
        net.broadcast(&Msg::Chat(line));
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
