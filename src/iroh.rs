use crate::id::PeerId;
use crate::message::{MessageReceived, P2PMessage};
use bevy::prelude::Resource;
use bevy_ecs::event::Event;
use bevy_ecs::message::MessageWriter;
use bevy_ecs::observer::On;
use bevy_ecs::system::{Commands, If, Res, ResMut};
use bevy_ecs::world::World;
use bevy_tokio_tasks::TokioTasksRuntime;
use bevy_tokio_tasks::tokio::io::{AsyncReadExt, AsyncWriteExt};
use bevy_tokio_tasks::tokio::sync::mpsc;
use bevy_tokio_tasks::tokio::sync::mpsc::{Receiver, Sender};
use bitcode::Buffer;
use iroh::Endpoint;
use iroh::endpoint::presets::N0;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};
use std::fmt::{Debug, Formatter};
use std::mem::transmute;
use std::slice;
use std::sync::Arc;
const ALPN: &[u8] = b"bevy_p2p";
const MESSAGE_MAIN: u8 = 0;
const MESSAGE_PEER_RELAY: u8 = 1;
#[derive(Resource)]
pub struct IrohResource<T: P2PMessage> {
    pub router: Router,
    pub connections: FxHashMap<PeerId, (Connection, SendStream)>,
    pub my_id: PeerId,
    buffer: Buffer,
    messages: Receiver<(PeerId, T)>,
    messages_send: Arc<Sender<(PeerId, T)>>,
    new_peers: Receiver<(Connection, SendStream)>,
    peer_relay: Receiver<(PeerId, Box<[PeerId]>)>,
    peer_relay_send: Arc<Sender<(PeerId, Box<[PeerId]>)>>,
}
#[derive(Event)]
pub struct IrohConnect {
    pub peer: PeerId,
}
impl IrohConnect {
    pub fn new(peer: PeerId) -> Self {
        Self { peer }
    }
}
pub(crate) fn on_connect<T: P2PMessage>(
    event: On<IrohConnect>,
    mut commands: Commands,
    tokio: Res<TokioTasksRuntime>,
    iroh_opt: Option<ResMut<IrohResource<T>>>,
) {
    tokio.runtime().block_on(async {
        if let Some(mut iroh) = iroh_opt {
            iroh.connect(event.peer).await;
        } else {
            let mut iroh = IrohResource::<T>::bind().await;
            iroh.connect(event.peer).await;
            commands.insert_resource(iroh);
        }
    });
}
#[derive(Event)]
pub struct IrohBind;
pub(crate) fn on_bind<T: P2PMessage>(
    _: On<IrohBind>,
    mut commands: Commands,
    tokio: Res<TokioTasksRuntime>,
    world: &World,
) {
    if !world.is_resource_added::<IrohResource<T>>() {
        tokio.runtime().block_on(async {
            let iroh = IrohResource::<T>::bind().await;
            commands.insert_resource(iroh);
        })
    }
}
#[derive(Event)]
pub struct IrohUnbind;
pub(crate) fn on_unbind<T: P2PMessage>(
    _: On<IrohUnbind>,
    mut commands: Commands,
    tokio: Res<TokioTasksRuntime>,
    iroh: If<ResMut<IrohResource<T>>>,
) {
    tokio
        .runtime()
        .block_on(async { iroh.router.shutdown().await.unwrap() });
    commands.remove_resource::<IrohResource<T>>();
}
impl<T: P2PMessage> IrohResource<T> {
    pub async fn bind() -> Self {
        let endpoint = Endpoint::bind(N0).await.unwrap();
        let my_id = PeerId::from(endpoint.id());
        let (new_tx, new_peers) = mpsc::channel(8);
        let (message_tx, messages) = mpsc::channel(4096);
        let (peer_tx, peer_relay) = mpsc::channel(8);
        let messages_send = Arc::new(message_tx);
        let peer_relay_send = Arc::new(peer_tx);
        let router = Router::builder(endpoint)
            .accept(
                ALPN,
                Protocol::new(new_tx, messages_send.clone(), peer_relay_send.clone()),
            )
            .spawn();
        let buffer = Buffer::new();
        let connections = FxHashMap::with_capacity_and_hasher(8, FxBuildHasher);
        Self {
            my_id,
            router,
            buffer,
            connections,
            new_peers,
            messages,
            messages_send,
            peer_relay,
            peer_relay_send,
        }
    }
    pub async fn connect(&mut self, peer: PeerId) {
        let connection = self
            .router
            .endpoint()
            .connect(peer.iroh(), ALPN)
            .await
            .unwrap();
        let (mut send, recv) = connection.open_bi().await.unwrap();
        send.write_all(&[]).await.unwrap();
        let peer = PeerId::from(connection.remote_id());
        bevy_tokio_tasks::tokio::spawn(receive(
            peer,
            recv,
            self.messages_send.clone(),
            self.peer_relay_send.clone(),
        ));
        self.connections.insert(peer, (connection, send));
    }
    pub async fn relay_peer(&mut self, new_peer: PeerId, checked_peers: &[PeerId]) {
        let mut set =
            FxHashSet::<PeerId>::with_capacity_and_hasher(checked_peers.len() + 1, FxBuildHasher);
        set.insert(new_peer);
        set.extend(checked_peers);
        let mut peers = Vec::with_capacity(self.connections.len() + 1);
        peers.push(new_peer);
        if self.connections.contains_key(&new_peer) {
            peers.extend(self.connections.keys().filter(|p| **p != new_peer));
        } else {
            self.connect(new_peer).await;
            peers.extend(self.connections.keys());
        }
        let (ptr, len, _) = peers.into_raw_parts();
        let buf = unsafe { slice::from_raw_parts(ptr.cast::<u8>(), len * size_of::<PeerId>()) };
        for (peer, (_, send)) in self.connections.iter_mut() {
            if !set.contains(peer) {
                send.write_u8(MESSAGE_PEER_RELAY).await.unwrap();
                send.write_u32(u32::try_from(len).unwrap()).await.unwrap();
                send.write_all(buf).await.unwrap();
            }
        }
    }
    pub async fn relay_peers(&mut self) {
        while let Ok((peer, peers)) = self.peer_relay.try_recv() {
            self.relay_peer(peer, &peers).await;
        }
    }
    pub async fn update(&mut self) {
        self.relay_peers().await;
        while let Ok((connection, reciever)) = self.new_peers.try_recv() {
            let peer = PeerId::from(connection.remote_id());
            self.relay_peer(peer, &[]).await;
            self.connections.insert(peer, (connection, reciever));
        }
    }
    pub async fn broadcast(&mut self, msg: T) {
        let bytes = self.buffer.encode(&msg);
        let len = u32::try_from(bytes.len()).unwrap();
        for (_, send) in self.connections.values_mut() {
            send.write_u8(MESSAGE_MAIN).await.unwrap();
            send.write_u32(len).await.unwrap();
            send.write_all(bytes).await.unwrap();
        }
    }
    pub async fn send(&mut self, peer: PeerId, msg: T) {
        if let Some((_, send)) = self.connections.get_mut(&peer) {
            let bytes = self.buffer.encode(&msg);
            let len = u32::try_from(bytes.len()).unwrap();
            send.write_u8(MESSAGE_MAIN).await.unwrap();
            send.write_u32(len).await.unwrap();
            send.write_all(bytes).await.unwrap();
        }
    }
}
struct Protocol<T: P2PMessage> {
    pub sender: Sender<(Connection, SendStream)>,
    pub messages: Arc<Sender<(PeerId, T)>>,
    pub peer_relay: Arc<Sender<(PeerId, Box<[PeerId]>)>>,
}
impl<T: P2PMessage> Debug for Protocol<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}
impl<T: P2PMessage> Protocol<T> {
    fn new(
        sender: Sender<(Connection, SendStream)>,
        messages: Arc<Sender<(PeerId, T)>>,
        peer_relay: Arc<Sender<(PeerId, Box<[PeerId]>)>>,
    ) -> Self {
        Self {
            sender,
            messages,
            peer_relay,
        }
    }
}
async fn receive<T: P2PMessage>(
    peer: PeerId,
    mut recv: RecvStream,
    send: Arc<Sender<(PeerId, T)>>,
    peer_relay: Arc<Sender<(PeerId, Box<[PeerId]>)>>,
) {
    let mut buffer = Buffer::new();
    let mut recv_buffer = Vec::new();
    while let Ok(message_type) = recv.read_u8().await
        && let Ok(size) = recv.read_u32().await
    {
        let len = size as usize;
        match message_type {
            MESSAGE_MAIN => {
                if len > recv_buffer.len() {
                    recv_buffer.resize(len, 0);
                }
                recv.read_exact(&mut recv_buffer[..len]).await.unwrap();
                let val = buffer.decode(&recv_buffer[..len]).unwrap();
                send.send((peer, val)).await.unwrap();
            }
            MESSAGE_PEER_RELAY => {
                let mut peer_buf = [0; size_of::<PeerId>()];
                let mut peers_buf = Vec::with_capacity(len * size_of::<PeerId>());
                recv.read_exact(&mut peer_buf).await.unwrap();
                recv.read_exact(&mut peers_buf).await.unwrap();
                let peer = unsafe { transmute::<[u8; size_of::<PeerId>()], PeerId>(peer_buf) };
                let (ptr, len, cap) = peers_buf.into_raw_parts();
                let peers = unsafe {
                    Vec::from_raw_parts(
                        ptr.cast::<PeerId>(),
                        len / size_of::<PeerId>(),
                        cap / size_of::<PeerId>(),
                    )
                };
                peer_relay
                    .send((peer, peers.into_boxed_slice()))
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
    }
}
impl<T: P2PMessage> ProtocolHandler for Protocol<T> {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (send, recv) = connection.accept_bi().await?;
        bevy_tokio_tasks::tokio::spawn(receive(
            PeerId::from(connection.remote_id()),
            recv,
            self.messages.clone(),
            self.peer_relay.clone(),
        ));
        self.sender.send((connection, send)).await.unwrap();
        Ok(())
    }
}
pub(crate) fn receive_messages<T: P2PMessage>(
    mut writer: MessageWriter<MessageReceived<T>>,
    mut iroh: If<ResMut<IrohResource<T>>>,
    tokio: Res<TokioTasksRuntime>,
) {
    tokio.runtime().block_on(async { iroh.update().await });
    while let Ok((peer, message)) = iroh.messages.try_recv() {
        writer.write(MessageReceived { peer, message });
    }
}
