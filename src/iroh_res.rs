use crate::message::{ConnectFailed, MessageReceived, P2PMessage, PeerConnected};
use bevy_ecs::event::Event;
use bevy_ecs::message::MessageWriter;
use bevy_ecs::observer::On;
use bevy_ecs::resource::Resource;
use bevy_ecs::system::{Commands, If, Res, ResMut};
use bevy_ecs::world::World;
use bevy_tokio_tasks::tokio::sync::mpsc;
use bevy_tokio_tasks::tokio::sync::mpsc::{Receiver, Sender};
use bevy_tokio_tasks::{TokioTasksRuntime, tokio};
use bitcode::Buffer;
use iroh::endpoint::presets::N0;
use iroh::endpoint::{BindError, Connection, ReadExactError, RecvStream, SendStream, WriteError};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointId};
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};
use std::fmt::{Debug, Formatter};
use std::io;
use std::sync::Arc;
use zerocopy::IntoBytes as _;
const ALPN: &[u8] = b"bevy_p2p";
#[derive(Resource)]
pub struct IrohResource<T: P2PMessage> {
    pub router: Router,
    pub connections: FxHashMap<EndpointId, (Connection, SendStream)>,
    pub pending: FxHashSet<EndpointId>,
    pub my_id: EndpointId,
    buffer: Buffer,
    new_peers: Receiver<(Connection, SendStream, bool)>,
    new_peers_send: Arc<Sender<(Connection, SendStream, bool)>>,
    messages: Receiver<(EndpointId, T)>,
    messages_send: Arc<Sender<(EndpointId, T)>>,
    peer_relay: Receiver<Box<[EndpointId]>>,
    peer_relay_send: Arc<Sender<Box<[EndpointId]>>>,
    peer_connect_failed: Receiver<EndpointId>,
    peer_connect_failed_send: Arc<Sender<EndpointId>>,
}
#[derive(Event)]
pub struct IrohConnect {
    pub peer: EndpointId,
}
impl IrohConnect {
    #[must_use]
    pub fn new(peer: EndpointId) -> Self {
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
            iroh.connect(event.peer);
        } else {
            let mut iroh = IrohResource::<T>::bind().await.unwrap();
            iroh.connect(event.peer);
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
        let iroh = tokio.runtime().block_on(IrohResource::<T>::bind()).unwrap();
        commands.insert_resource(iroh);
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
    tokio.runtime().block_on(iroh.router.shutdown()).unwrap();
    commands.remove_resource::<IrohResource<T>>();
}
impl<T: P2PMessage> IrohResource<T> {
    pub async fn bind() -> Result<Self, BindError> {
        let endpoint = Endpoint::bind(N0).await?;
        let my_id = EndpointId::from(endpoint.id());
        let (new_tx, new_peers) = mpsc::channel(8);
        let (message_tx, messages) = mpsc::channel(4096);
        let (peer_tx, peer_relay) = mpsc::channel(8);
        let (peer_connect_failed_tx, peer_connect_failed) = mpsc::channel(8);
        let messages_send = Arc::new(message_tx);
        let new_peers_send = Arc::new(new_tx);
        let peer_relay_send = Arc::new(peer_tx);
        let peer_connect_failed_send = Arc::new(peer_connect_failed_tx);
        let router = Router::builder(endpoint)
            .accept(
                ALPN,
                Protocol::new(
                    new_peers_send.clone(),
                    messages_send.clone(),
                    peer_relay_send.clone(),
                ),
            )
            .spawn();
        let buffer = Buffer::new();
        let connections = FxHashMap::with_capacity_and_hasher(8, FxBuildHasher);
        let pending = FxHashSet::with_capacity_and_hasher(8, FxBuildHasher);
        Ok(Self {
            router,
            connections,
            pending,
            my_id,
            buffer,
            new_peers,
            new_peers_send,
            messages,
            messages_send,
            peer_relay,
            peer_relay_send,
            peer_connect_failed,
            peer_connect_failed_send,
        })
    }
    pub fn connect(&mut self, peer: EndpointId) {
        async fn connect<K: P2PMessage>(
            peer: EndpointId,
            endpoint: Endpoint,
            sender: Arc<Sender<(Connection, SendStream, bool)>>,
            messages_send: Arc<Sender<(EndpointId, K)>>,
            peer_relay_send: Arc<Sender<Box<[EndpointId]>>>,
            peer_connect_failed: Arc<Sender<EndpointId>>,
        ) {
            match endpoint.connect(peer, ALPN).await {
                Ok(connection) => {
                    let (send, recv) = connection.open_bi().await.unwrap();
                    tokio::spawn(receive(peer, recv, messages_send, peer_relay_send));
                    sender.send((connection, send, true)).await.unwrap();
                }
                Err(_) => {
                    peer_connect_failed.send(peer).await.unwrap();
                }
            }
        }
        if self.connections.contains_key(&peer) || self.pending.contains(&peer) {
            return;
        }
        self.pending.insert(peer);
        tokio::spawn(connect(
            peer,
            self.router.endpoint().clone(),
            self.new_peers_send.clone(),
            self.messages_send.clone(),
            self.peer_relay_send.clone(),
            self.peer_connect_failed_send.clone(),
        ));
    }
    pub async fn relay_peer(&mut self, send: &mut SendStream) -> Result<(), io::Error> {
        let len = u32::try_from(self.connections.len()).unwrap();
        send.write_all(len.as_bytes()).await?;
        for peer in self.connections.keys() {
            send.write_all(peer.as_bytes()).await?;
        }
        Ok(())
    }
    pub async fn update(&mut self, mut f: impl FnMut(EndpointId)) {
        while let Ok((connection, mut send, owner)) = self.new_peers.try_recv() {
            let peer = EndpointId::from(connection.remote_id());
            if self.connections.contains_key(&peer) {
                if (self.my_id < peer) ^ owner {
                    continue;
                }
            } else {
                f(peer);
            }
            if self.relay_peer(&mut send).await.is_ok() {
                self.connections.insert(peer, (connection, send));
            }
            self.pending.remove(&peer);
        }
        while let Ok(peers) = self.peer_relay.try_recv() {
            for peer in peers {
                if peer != self.my_id {
                    self.connect(peer);
                }
            }
        }
    }
    pub async fn broadcast(&mut self, msg: &T, mut f: impl FnMut(EndpointId)) {
        let bytes = self.buffer.encode(msg);
        let mut disconnections = Vec::with_capacity(4);
        for (peer, (_, send)) in &mut self.connections {
            if send_bytes(send, bytes).await.is_err() {
                disconnections.push(*peer);
            }
        }
        for peer in disconnections {
            self.connections.remove(&peer);
            f(peer);
        }
    }
    pub async fn send(&mut self, peer: EndpointId, msg: &T, f: impl FnOnce(EndpointId)) {
        if let Some((_, send)) = self.connections.get_mut(&peer) {
            let bytes = self.buffer.encode(msg);
            if send_bytes(send, bytes).await.is_err() {
                self.connections.remove(&peer);
                f(peer);
            }
        }
    }
}
async fn send_bytes(send: &mut SendStream, bytes: &[u8]) -> Result<(), WriteError> {
    let len = u32::try_from(bytes.len()).unwrap();
    send.write_all(len.as_bytes()).await?;
    send.write_all(bytes).await
}
struct Protocol<T: P2PMessage> {
    pub sender: Arc<Sender<(Connection, SendStream, bool)>>,
    pub messages: Arc<Sender<(EndpointId, T)>>,
    pub peer_relay: Arc<Sender<Box<[EndpointId]>>>,
}
impl<T: P2PMessage> Debug for Protocol<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Protocol")
    }
}
impl<T: P2PMessage> Protocol<T> {
    fn new(
        sender: Arc<Sender<(Connection, SendStream, bool)>>,
        messages: Arc<Sender<(EndpointId, T)>>,
        peer_relay: Arc<Sender<Box<[EndpointId]>>>,
    ) -> Self {
        Self {
            sender,
            messages,
            peer_relay,
        }
    }
}
async fn read_u32(recv: &mut RecvStream) -> Result<u32, ReadExactError> {
    let mut val = 0;
    recv.read_exact(val.as_mut_bytes()).await?;
    Ok(val)
}
async fn receive<T: P2PMessage>(
    peer: EndpointId,
    mut recv: RecvStream,
    send: Arc<Sender<(EndpointId, T)>>,
    peer_relay: Arc<Sender<Box<[EndpointId]>>>,
) -> Result<(), ReadExactError> {
    let size = read_u32(&mut recv).await?;
    if size != 0 {
        let len = size as usize;
        let mut peers_buf = vec![0; len * size_of::<EndpointId>()];
        recv.read_exact(&mut peers_buf).await?;
        let (ptr, len, cap) = peers_buf.into_raw_parts();
        let peers = unsafe {
            Vec::from_raw_parts(
                ptr.cast::<EndpointId>(),
                len / size_of::<EndpointId>(),
                cap / size_of::<EndpointId>(),
            )
        };
        peer_relay.send(peers.into_boxed_slice()).await.unwrap();
    }
    let mut buffer = Buffer::new();
    let mut recv_buffer = Vec::new();
    while let Ok(size) = read_u32(&mut recv).await {
        let len = size as usize;
        if len > recv_buffer.len() {
            recv_buffer.resize(len, 0);
        }
        recv.read_exact(&mut recv_buffer[..len]).await?;
        let val = buffer.decode(&recv_buffer[..len]).unwrap();
        send.send((peer, val)).await.unwrap();
    }
    Ok(())
}
impl<T: P2PMessage> ProtocolHandler for Protocol<T> {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (send, recv) = connection.accept_bi().await?;
        tokio::spawn(receive(
            EndpointId::from(connection.remote_id()),
            recv,
            self.messages.clone(),
            self.peer_relay.clone(),
        ));
        self.sender.send((connection, send, false)).await.unwrap();
        Ok(())
    }
}
pub(crate) fn receive_messages<T: P2PMessage>(
    mut writer: MessageWriter<MessageReceived<T>>,
    mut peer_writer: MessageWriter<PeerConnected>,
    mut peer_failed_writer: MessageWriter<ConnectFailed>,
    mut iroh: If<ResMut<IrohResource<T>>>,
    tokio: Res<TokioTasksRuntime>,
) {
    tokio.runtime().block_on(iroh.update(|peer| {
        peer_writer.write(PeerConnected::from(peer));
    }));
    while let Ok((peer, message)) = iroh.messages.try_recv() {
        writer.write(MessageReceived { peer, message });
    }
    while let Ok(peer) = iroh.peer_connect_failed.try_recv() {
        peer_failed_writer.write(ConnectFailed::from(peer));
    }
}
