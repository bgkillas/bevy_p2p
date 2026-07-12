use bevy::MinimalPlugins;
use bevy::app::{App, FixedUpdate};
use bevy_app::Startup;
use bevy_ecs::message::PopulatedMessageReader;
use bevy_ecs::resource::Resource;
use bevy_ecs::system::{Commands, Res};
use bevy_p2p::id::PeerId;
use bevy_p2p::iroh::{IrohBind, IrohConnect, IrohResource};
use bevy_p2p::message::{MessageReceived, Net, PeerJoined};
use bevy_p2p::plugin::P2PPlugin;
use bevy_tokio_tasks::TokioTasksPlugin;
use bitcode::{Decode, Encode};
use iroh::EndpointId;
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
    app.add_plugins(TokioTasksPlugin::default());
    app.world_mut().trigger(IrohBind);
    app.insert_resource(Lines { rx: Mutex::new(rx) });
    app.add_systems(Startup, startup);
    app.add_systems(FixedUpdate, (update, on_join, receive_message));
    app.run();
}
fn on_join(mut reader: PopulatedMessageReader<PeerJoined>) {
    for peer in reader.read() {
        println!("{} joined", peer.peer.iroh().fmt_short());
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
            let peer = PeerId::from(endpoint);
            commands.trigger(IrohConnect::new(peer));
        }
    }
    file.write_fmt(format_args!("{}\n", iroh.router.endpoint().id()))
        .unwrap();
    println!("{}", iroh.router.endpoint().id().fmt_short());
}
fn update(mut net: Net<Msg>, rx: Res<Lines>) {
    if let Ok(line) = rx.rx.lock().unwrap().try_recv() {
        net.broadcast(&Msg::Chat(line)).unwrap()
    }
}
fn receive_message(mut reader: PopulatedMessageReader<MessageReceived<Msg>>) {
    for msg in reader.read() {
        match &msg.message {
            Msg::Chat(str) => {
                println!("{}: {str}", msg.peer.iroh().fmt_short());
            }
        }
    }
}
#[derive(Encode, Decode)]
pub enum Msg {
    Chat(String),
}
