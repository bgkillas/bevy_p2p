use crate::events::{Binded, ConnectFailed, PeerConnected, PeerDisconnected};
use crate::message::{MessageReceived, P2PMessage};
use crate::runtime::Runtime;
use bevy_ecs::event::Event;
use bevy_ecs::message::MessageWriter;
use bevy_ecs::observer::On;
use bevy_ecs::resource::Resource;
use bevy_ecs::system::{Commands, If, In, Res};
use bevy_ecs::world::World;
use bimap::BiHashMap;
use bitcode::Buffer;
use iroh::endpoint::presets::N0;
use iroh::endpoint::{BindError, Connection, ReadExactError, RecvStream, SendStream, WriteError};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointId};
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};
use std::fmt::{Debug, Formatter};
use std::io;
use std::sync::Arc;
use std::sync::mpmc::{self, Receiver, Sender};
use tokio::spawn;
use tokio::sync::Mutex;
#[cfg(target_family = "wasm")]
use tokio_with_wasm as tokio;
use zerocopy::IntoBytes as _;
#[derive(Clone, Copy, Eq, Hash, PartialEq, PartialOrd, Debug)]
pub struct PeerId {
    pub id: u32,
}
#[derive(Resource)]
pub struct IrohResource<T: P2PMessage> {
    pub inner: Arc<Mutex<IrohInner<T>>>,
    pub messages: Receiver<(EndpointId, T)>,
    pub peer_connect_failed: Receiver<EndpointId>,
    pub peer_disconnects: Receiver<EndpointId>,
    pub peer_connected: Receiver<EndpointId>,
    pub my_id: EndpointId,
    pub my_peer_id: Option<PeerId>,
    pub peer_ids: BiHashMap<EndpointId, PeerId>,
}
pub struct IrohInner<T: P2PMessage> {
    pub alpn: &'static [u8],
    pub router: Router,
    pub connections: FxHashMap<EndpointId, (Connection, SendStream)>,
    pub pending: FxHashSet<EndpointId>,
    pub my_id: EndpointId,
    pub my_peer_id: Option<PeerId>,
    pub peer_ids: BiHashMap<EndpointId, PeerId>,
    buffer: Buffer,
    new_peers: Receiver<(Connection, SendStream, bool)>,
    new_peers_send: Sender<(Connection, SendStream, bool)>,
    messages_send: Sender<(EndpointId, T)>,
    peer_relay: Receiver<Box<[EndpointId]>>,
    peer_relay_send: Sender<Box<[EndpointId]>>,
    peer_connect_failed_send: Sender<EndpointId>,
    peer_disconnects_send: Sender<EndpointId>,
    peer_connected_send: Sender<EndpointId>,
}
#[derive(Event)]
pub struct IrohConnect {
    pub peer: EndpointId,
    pub alpn: &'static [u8],
}
impl IrohConnect {
    #[must_use]
    pub fn new(peer: EndpointId, alpn: &'static [u8]) -> Self {
        Self { peer, alpn }
    }
}
pub(crate) fn on_connect<T: P2PMessage>(
    event: On<IrohConnect>,
    runtime: Res<Runtime>,
    iroh_opt: Option<Res<IrohResource<T>>>,
) {
    let peer = event.peer;
    let alpn = event.alpn;
    if let Some(iroh) = iroh_opt {
        let inner = iroh.inner.clone();
        runtime.spawn(async move {
            inner.lock().await.connect(alpn, peer);
        });
    } else {
        runtime.spawn_hook(insert_iroh, async move {
            match IrohResource::<T>::bind(alpn).await {
                Ok(iroh) => {
                    iroh.inner.lock().await.connect(alpn, peer);
                    Ok(iroh)
                }
                e => e,
            }
        });
    }
}
#[derive(Event)]
pub struct IrohBind {
    alpn: &'static [u8],
}
impl IrohBind {
    #[must_use]
    pub fn new(alpn: &'static [u8]) -> Self {
        Self { alpn }
    }
}
pub(crate) fn on_bind<T: P2PMessage>(event: On<IrohBind>, runtime: Res<Runtime>, world: &World) {
    assert!(!world.is_resource_added::<IrohResource<T>>());
    runtime.spawn_hook(insert_iroh, IrohResource::<T>::bind(event.alpn));
}
fn insert_iroh<T: P2PMessage>(
    In(iroh): In<Result<IrohResource<T>, BindError>>,
    mut commands: Commands,
) {
    commands.insert_resource(iroh.unwrap());
    commands.trigger(Binded);
}
#[derive(Event)]
pub struct IrohUnbind;
pub(crate) fn on_unbind<T: P2PMessage>(
    _: On<IrohUnbind>,
    runtime: Res<Runtime>,
    iroh: If<Res<IrohResource<T>>>,
) {
    let inner = iroh.inner.clone();
    runtime.spawn_hook(remove_iroh::<T, _>, async move {
        inner.lock().await.router.shutdown().await
    });
}
fn remove_iroh<T: P2PMessage, E: Debug>(In(res): In<Result<(), E>>, mut commands: Commands) {
    res.unwrap();
    commands.remove_resource::<IrohResource<T>>();
}
impl<T: P2PMessage> IrohResource<T> {
    pub async fn bind(alpn: &'static [u8]) -> Result<Self, BindError> {
        let endpoint = Endpoint::bind(N0).await?;
        let my_id = EndpointId::from(endpoint.id());
        let (new_peers_send, new_peers) = mpmc::channel();
        let (messages_send, messages) = mpmc::channel();
        let (peer_relay_send, peer_relay) = mpmc::channel();
        let (peer_connect_failed_send, peer_connect_failed) = mpmc::channel();
        let (peer_disconnects_send, peer_disconnects) = mpmc::channel();
        let (peer_connected_send, peer_connected) = mpmc::channel();
        let router = Router::builder(endpoint)
            .accept(
                alpn,
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
        let inner = Arc::new(Mutex::new(IrohInner {
            alpn,
            router,
            connections,
            pending,
            my_id,
            my_peer_id: None,
            peer_ids: BiHashMap::new(),
            buffer,
            new_peers,
            new_peers_send,
            messages_send,
            peer_relay,
            peer_relay_send,
            peer_connect_failed_send,
            peer_disconnects_send,
            peer_connected_send,
        }));
        Ok(Self {
            inner,
            messages,
            peer_connect_failed,
            peer_disconnects,
            peer_connected,
            my_id,
            my_peer_id: None,
            peer_ids: BiHashMap::new(),
        })
    }
}
impl<T: P2PMessage> IrohInner<T> {
    pub fn connect(&mut self, alpn: &'static [u8], peer: EndpointId) {
        async fn connect<K: P2PMessage>(
            alpn: &'static [u8],
            peer: EndpointId,
            endpoint: Endpoint,
            sender: Sender<(Connection, SendStream, bool)>,
            messages_send: Sender<(EndpointId, K)>,
            peer_relay_send: Sender<Box<[EndpointId]>>,
            peer_connect_failed: Sender<EndpointId>,
        ) {
            match endpoint.connect(peer, alpn).await {
                Ok(connection) => {
                    let (send, recv) = connection.open_bi().await.unwrap();
                    spawn(receive(peer, recv, messages_send, peer_relay_send));
                    sender.send((connection, send, true)).unwrap();
                }
                Err(_) => {
                    peer_connect_failed.send(peer).unwrap();
                }
            }
        }
        if self.connections.contains_key(&peer) || self.pending.contains(&peer) {
            return;
        }
        self.pending.insert(peer);
        spawn(connect(
            alpn,
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
    pub async fn update(&mut self) {
        while let Ok((connection, mut send, owner)) = self.new_peers.try_recv() {
            let peer = EndpointId::from(connection.remote_id());
            if self.connections.contains_key(&peer) {
                if (self.my_id < peer) ^ owner {
                    continue;
                }
            } else {
                self.peer_connected_send.send(peer).unwrap();
            }
            if self.relay_peer(&mut send).await.is_ok() {
                self.connections.insert(peer, (connection, send));
            }
            self.pending.remove(&peer);
        }
        while let Ok(peers) = self.peer_relay.try_recv() {
            for peer in peers {
                if peer != self.my_id {
                    self.connect(self.alpn, peer);
                }
            }
        }
    }
    pub async fn broadcast(&mut self, msg: &T) {
        let bytes = self.buffer.encode(msg);
        let mut disconnections = Vec::with_capacity(4);
        for (peer, (_, send)) in &mut self.connections {
            if send_bytes(send, bytes).await.is_err() {
                disconnections.push(*peer);
            }
        }
        for peer in disconnections {
            self.connections.remove(&peer);
            self.peer_disconnects_send.send(peer).unwrap();
        }
    }
    pub async fn send(&mut self, peer: EndpointId, msg: &T) {
        if let Some((_, send)) = self.connections.get_mut(&peer) {
            let bytes = self.buffer.encode(msg);
            if send_bytes(send, bytes).await.is_err() {
                self.connections.remove(&peer);
                self.peer_disconnects_send.send(peer).unwrap();
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
    pub sender: Sender<(Connection, SendStream, bool)>,
    pub messages: Sender<(EndpointId, T)>,
    pub peer_relay: Sender<Box<[EndpointId]>>,
}
impl<T: P2PMessage> Debug for Protocol<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Protocol")
    }
}
impl<T: P2PMessage> Protocol<T> {
    fn new(
        sender: Sender<(Connection, SendStream, bool)>,
        messages: Sender<(EndpointId, T)>,
        peer_relay: Sender<Box<[EndpointId]>>,
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
    send: Sender<(EndpointId, T)>,
    peer_relay: Sender<Box<[EndpointId]>>,
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
        peer_relay.send(peers.into_boxed_slice()).unwrap();
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
        send.send((peer, val)).unwrap();
    }
    Ok(())
}
impl<T: P2PMessage> ProtocolHandler for Protocol<T> {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (send, recv) = connection.accept_bi().await?;
        spawn(receive(
            EndpointId::from(connection.remote_id()),
            recv,
            self.messages.clone(),
            self.peer_relay.clone(),
        ));
        self.sender.send((connection, send, false)).unwrap();
        Ok(())
    }
}
pub(crate) fn receive_messages<T: P2PMessage>(
    mut writer: MessageWriter<MessageReceived<T>>,
    iroh: If<Res<IrohResource<T>>>,
    runtime: Res<Runtime>,
    mut commands: Commands,
) {
    let clone = iroh.inner.clone();
    runtime.spawn(async move {
        clone.lock().await.update().await;
    });
    while let Ok((peer, message)) = iroh.messages.try_recv() {
        writer.write(MessageReceived { peer, message });
    }
    while let Ok(peer) = iroh.peer_disconnects.try_recv() {
        commands.trigger(PeerDisconnected { peer });
    }
    while let Ok(peer) = iroh.peer_connected.try_recv() {
        commands.trigger(PeerConnected { peer });
    }
    while let Ok(peer) = iroh.peer_connect_failed.try_recv() {
        commands.trigger(ConnectFailed { peer });
    }
}
