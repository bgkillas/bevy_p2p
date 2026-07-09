use crate::id::PeerId;
use crate::message::{MessageBroadcast, MessageReceived, MessageTo, P2PMessage};
use bevy::prelude::Resource;
use bevy_ecs::event::Event;
use bevy_ecs::message::{MessageWriter, PopulatedMessageReader};
use bevy_ecs::observer::On;
use bevy_ecs::system::{Commands, If, ResMut};
use bevy_log::info;
use bevy_tokio_tasks::TokioTasksRuntime;
use bevy_tokio_tasks::tokio::io::{AsyncReadExt, AsyncWriteExt};
use bevy_tokio_tasks::tokio::sync::mpsc;
use bevy_tokio_tasks::tokio::sync::mpsc::{Receiver, Sender};
use bitcode::Buffer;
use iroh::endpoint::presets::N0;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointId};
use rustc_hash::{FxBuildHasher, FxHashMap};
use std::fmt::{Debug, Formatter};
const ALPN: &[u8] = b"bevy_p2p";
#[derive(Resource)]
pub struct IrohResource<T: P2PMessage> {
    pub router: Router,
    buffer: Buffer,
    pub connections: FxHashMap<PeerId, (Connection, SendStream, Receiver<T>)>,
    receiver: Receiver<(Connection, SendStream, Receiver<T>)>,
}
#[derive(Event)]
pub struct IrohConnect {
    pub endpoint: EndpointId,
}
impl IrohConnect {
    pub fn new(endpoint: EndpointId) -> Self {
        Self { endpoint }
    }
}
pub(crate) fn on_connect<T: P2PMessage>(
    event: On<IrohConnect>,
    mut commands: Commands,
    mut tokio: ResMut<TokioTasksRuntime>,
    iroh_opt: Option<ResMut<IrohResource<T>>>,
) {
    if let Some(mut iroh) = iroh_opt {
        iroh.connect(&mut tokio, event.endpoint);
    } else {
        let mut iroh = IrohResource::<T>::bind(&mut tokio);
        iroh.connect(&mut tokio, event.endpoint);
        commands.insert_resource(iroh);
    }
}
#[derive(Event)]
pub struct IrohBind;
pub(crate) fn on_bind<T: P2PMessage>(
    _: On<IrohBind>,
    mut commands: Commands,
    mut tokio: ResMut<TokioTasksRuntime>,
) {
    let iroh = IrohResource::<T>::bind(&mut tokio);
    commands.insert_resource(iroh);
}
impl<T: P2PMessage> IrohResource<T> {
    pub fn bind(runtime: &mut TokioTasksRuntime) -> Self {
        runtime.runtime().block_on(async {
            let endpoint = Endpoint::bind(N0).await.unwrap();
            let (tx, receiver) = mpsc::channel(256);
            let router = Router::builder(endpoint)
                .accept(ALPN, Protocol::new(tx))
                .spawn();
            let buffer = Buffer::new();
            let connections = FxHashMap::with_capacity_and_hasher(256, FxBuildHasher);
            Self {
                router,
                buffer,
                connections,
                receiver,
            }
        })
    }
    pub fn connect(&mut self, runtime: &mut TokioTasksRuntime, addr: EndpointId) {
        runtime.runtime().block_on(async {
            let connection = self.router.endpoint().connect(addr, ALPN).await.unwrap();
            info!("connecting: {}", connection.remote_id());
            let (mut send, recv) = connection.open_bi().await.unwrap();
            send.write(&[]).await.unwrap();
            info!("connected: {}", connection.remote_id());
            let (tx, rx) = mpsc::channel(256);
            bevy_tokio_tasks::tokio::spawn(receive(recv, tx));
            self.connections
                .insert(PeerId::from(connection.remote_id()), (connection, send, rx));
        });
    }
    pub fn broadcast<'a>(
        &mut self,
        runtime: &mut TokioTasksRuntime,
        msgs: impl Iterator<Item = &'a T>,
    ) {
        runtime.runtime().block_on(async {
            for msg in msgs {
                let bytes = self.buffer.encode(msg);
                for (_, send, _) in self.connections.values_mut() {
                    send.write_u32(bytes.len() as u32).await.unwrap();
                    send.write(bytes).await.unwrap();
                }
            }
        });
    }
    pub fn send<'a>(
        &mut self,
        runtime: &mut TokioTasksRuntime,
        msgs: impl Iterator<Item = (PeerId, &'a T)>,
    ) {
        runtime.runtime().block_on(async {
            for (peer, msg) in msgs {
                if let Some((_, send, _)) = self.connections.get_mut(&peer) {
                    let bytes = self.buffer.encode(msg);
                    send.write_u32(bytes.len() as u32).await.unwrap();
                    send.write(bytes).await.unwrap();
                }
            }
        });
    }
    pub fn receive(&mut self, runtime: &mut TokioTasksRuntime, mut f: impl FnMut(EndpointId, T)) {
        runtime.runtime().block_on(async {
            for (conn, _, recv) in self.connections.values_mut() {
                let peer = conn.remote_id();
                while let Ok(val) = recv.try_recv() {
                    f(peer, val);
                }
            }
        });
    }
}
struct Protocol<T: P2PMessage> {
    pub sender: Sender<(Connection, SendStream, Receiver<T>)>,
}
impl<T: P2PMessage> Debug for Protocol<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Protocol({:?})", self.sender)
    }
}
impl<T: P2PMessage> Protocol<T> {
    fn new(sender: Sender<(Connection, SendStream, Receiver<T>)>) -> Self {
        Self { sender }
    }
}
async fn receive<T: P2PMessage>(mut recv: RecvStream, send: Sender<T>) {
    let mut buffer = Buffer::new();
    let mut recv_buffer = Vec::new();
    while let Ok(size) = recv.read_u32().await {
        let len = size as usize;
        if len > recv_buffer.len() {
            recv_buffer.resize(len, 0);
        }
        recv.read_exact(&mut recv_buffer[..len]).await.unwrap();
        let val = buffer.decode(&recv_buffer[..len]).unwrap();
        send.send(val).await.unwrap();
    }
}
impl<T: P2PMessage> ProtocolHandler for Protocol<T> {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (send, recv) = connection.accept_bi().await?;
        info!("connected: {}", connection.remote_id());
        let (tx, rx) = mpsc::channel(256);
        bevy_tokio_tasks::tokio::spawn(receive(recv, tx));
        self.sender.send((connection, send, rx)).await.unwrap();
        Ok(())
    }
}
pub(crate) fn message_to<T: P2PMessage>(
    mut tokio: ResMut<TokioTasksRuntime>,
    mut reader: PopulatedMessageReader<MessageTo<T>>,
    mut iroh: If<ResMut<IrohResource<T>>>,
) {
    iroh.send(&mut tokio, reader.read().map(|m| (m.peer_id, &m.message)))
}
pub(crate) fn message_broadcast<T: P2PMessage>(
    mut tokio: ResMut<TokioTasksRuntime>,
    mut reader: PopulatedMessageReader<MessageBroadcast<T>>,
    mut iroh: If<ResMut<IrohResource<T>>>,
) {
    iroh.broadcast(&mut tokio, reader.read().map(|m| &m.message))
}
pub(crate) fn receive_messages<T: P2PMessage>(
    mut tokio: ResMut<TokioTasksRuntime>,
    mut writer: MessageWriter<MessageReceived<T>>,
    mut iroh: If<ResMut<IrohResource<T>>>,
) {
    while let Ok((connection, reciever, sender)) = iroh.receiver.try_recv() {
        let peer = PeerId::from(connection.remote_id());
        iroh.connections
            .insert(peer, (connection, reciever, sender));
    }
    iroh.receive(&mut tokio, |peer, message| {
        writer.write(MessageReceived {
            peer: PeerId::from(peer),
            message,
        });
    });
}
