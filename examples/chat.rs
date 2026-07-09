use bevy::MinimalPlugins;
use bevy::app::{App, FixedUpdate};
use bevy_ecs::message::PopulatedMessageReader;
use bevy_ecs::resource::Resource;
use bevy_ecs::system::Res;
use bevy_p2p::iroh::IrohBind;
use bevy_p2p::message::{MessageReceived, Net};
use bevy_p2p::plugin::P2PPlugin;
use bevy_tokio_tasks::TokioTasksPlugin;
use bitcode::{Decode, Encode};
use std::io::{BufRead, stdin};
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
    app.add_systems(FixedUpdate, (update, receive_message));
    app.run();
}
fn update(mut net: Net<Msg>, rx: Res<Lines>) {
    if let Ok(line) = rx.rx.lock().unwrap().try_recv() {
        println!("sending: {line}");
        net.broadcast(Msg::Chat(line))
    }
}
fn receive_message(mut reader: PopulatedMessageReader<MessageReceived<Msg>>) {
    for msg in reader.read() {
        match &msg.message {
            Msg::Chat(str) => {
                println!("{:?}: {str}", msg.peer);
            }
        }
    }
}
#[derive(Encode, Decode)]
pub enum Msg {
    Chat(String),
}
