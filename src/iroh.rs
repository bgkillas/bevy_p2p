use crate::id::PeerId;
use crate::message::{MessageReceived, P2PMessage, PeerJoined};
use bevy::prelude::Resource;
use bevy_ecs::event::Event;
use bevy_ecs::message::MessageWriter;
use bevy_ecs::observer::On;
use bevy_ecs::system::{Commands, If, Res, ResMut};
use bevy_ecs::world::World;
use bevy_tokio_tasks::tokio::sync::mpsc;
use bevy_tokio_tasks::tokio::sync::mpsc::{Receiver, Sender};
use bevy_tokio_tasks::{TokioTasksRuntime, tokio};
use bitcode::Buffer;
use iroh::Endpoint;
use iroh::endpoint::presets::N0;
use iroh::endpoint::{BindError, Connection, ReadExactError, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};
use std::fmt::{Debug, Formatter};
use std::io;
use std::sync::Arc;
use zerocopy::IntoBytes as _;
const ALPN: &[u8] = b"bevy_p2p";
#[derive(Resource)]
pub struct IrohResource<T: P2PMessage> {
    pub router: Router,
    pub connections: FxHashMap<PeerId, (Connection, SendStream)>,
    pub pending: FxHashSet<PeerId>,
    pub my_id: PeerId,
    buffer: Buffer,
    new_peers: Receiver<(Connection, SendStream, bool)>,
    new_peers_send: Arc<Sender<(Connection, SendStream, bool)>>,
    messages: Receiver<(PeerId, T)>,
    messages_send: Arc<Sender<(PeerId, T)>>,
    peer_relay: Receiver<Box<[PeerId]>>,
    peer_relay_send: Arc<Sender<Box<[PeerId]>>>,
}
#[derive(Event)]
pub struct IrohConnect {
    pub peer: PeerId,
}
impl IrohConnect {
    #[must_use]
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
        if let Err(e) = try {
            if let Some(mut iroh) = iroh_opt {
                iroh.connect(event.peer)?;
            } else {
                let mut iroh = IrohResource::<T>::bind().await.unwrap();
                iroh.connect(event.peer)?;
                commands.insert_resource(iroh);
            }
        } {
            println!("{e:?}");
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
        let my_id = PeerId::from(endpoint.id());
        let (new_tx, new_peers) = mpsc::channel(8);
        let (message_tx, messages) = mpsc::channel(4096);
        let (peer_tx, peer_relay) = mpsc::channel(8);
        let messages_send = Arc::new(message_tx);
        let new_peers_send = Arc::new(new_tx);
        let peer_relay_send = Arc::new(peer_tx);
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
        })
    }
    pub fn connect(&mut self, peer: PeerId) -> Result<(), io::Error> {
        async fn connect<K: P2PMessage>(
            peer: PeerId,
            endpoint: Endpoint,
            sender: Arc<Sender<(Connection, SendStream, bool)>>,
            messages_send: Arc<Sender<(PeerId, K)>>,
            peer_relay_send: Arc<Sender<Box<[PeerId]>>>,
        ) {
            if let Ok(connection) = endpoint.connect(peer.iroh(), ALPN).await {
                let (send, recv) = connection.open_bi().await.unwrap();
                tokio::spawn(receive(peer, recv, messages_send, peer_relay_send));
                sender.send((connection, send, true)).await.unwrap();
            }
        }
        if self.connections.contains_key(&peer) || self.pending.contains(&peer) {
            return Ok(());
        }
        self.pending.insert(peer);
        tokio::spawn(connect(
            peer,
            self.router.endpoint().clone(),
            self.new_peers_send.clone(),
            self.messages_send.clone(),
            self.peer_relay_send.clone(),
        ));
        Ok(())
    }
    pub async fn relay_peer(&mut self, send: &mut SendStream) -> Result<(), io::Error> {
        let len = u32::try_from(self.connections.len()).unwrap();
        send.write_all(len.as_bytes()).await?;
        for peer in self.connections.keys().copied() {
            send.write_all(peer.iroh().as_bytes()).await?;
        }
        Ok(())
    }
    pub async fn update(&mut self, mut f: impl FnMut(PeerId)) -> Result<(), io::Error> {
        while let Ok((connection, mut send, owner)) = self.new_peers.try_recv() {
            let peer = PeerId::from(connection.remote_id());
            if self.connections.contains_key(&peer) {
                if (self.my_id.iroh() < peer.iroh()) ^ owner {
                    continue;
                }
            } else {
                f(peer);
            }
            self.relay_peer(&mut send).await?;
            self.connections.insert(peer, (connection, send));
            self.pending.remove(&peer);
        }
        while let Ok(peers) = self.peer_relay.try_recv() {
            for peer in peers {
                self.connect(peer)?;
            }
        }
        Ok(())
    }
    pub async fn broadcast(&mut self, msg: &T) -> Result<(), io::Error> {
        let bytes = self.buffer.encode(msg);
        let len = u32::try_from(bytes.len()).unwrap();
        for (_, send) in self.connections.values_mut() {
            send.write_all(len.as_bytes()).await?;
            send.write_all(bytes).await?;
        }
        Ok(())
    }
    pub async fn send(&mut self, peer: PeerId, msg: &T) -> Result<(), io::Error> {
        if let Some((_, send)) = self.connections.get_mut(&peer) {
            let bytes = self.buffer.encode(msg);
            let len = u32::try_from(bytes.len()).unwrap();
            send.write_all(len.as_bytes()).await?;
            send.write_all(bytes).await?;
        }
        Ok(())
    }
}
struct Protocol<T: P2PMessage> {
    pub sender: Arc<Sender<(Connection, SendStream, bool)>>,
    pub messages: Arc<Sender<(PeerId, T)>>,
    pub peer_relay: Arc<Sender<Box<[PeerId]>>>,
}
impl<T: P2PMessage> Debug for Protocol<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Protocol")
    }
}
impl<T: P2PMessage> Protocol<T> {
    fn new(
        sender: Arc<Sender<(Connection, SendStream, bool)>>,
        messages: Arc<Sender<(PeerId, T)>>,
        peer_relay: Arc<Sender<Box<[PeerId]>>>,
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
    peer: PeerId,
    mut recv: RecvStream,
    send: Arc<Sender<(PeerId, T)>>,
    peer_relay: Arc<Sender<Box<[PeerId]>>>,
) {
    if let Err(e) = try {
        let size = read_u32(&mut recv).await?;
        if size != 0 {
            let len = size as usize;
            let mut peers_buf = vec![0; len * size_of::<PeerId>()];
            recv.read_exact(&mut peers_buf).await?;
            let (ptr, len, cap) = peers_buf.into_raw_parts();
            let peers = unsafe {
                Vec::from_raw_parts(
                    ptr.cast::<PeerId>(),
                    len / size_of::<PeerId>(),
                    cap / size_of::<PeerId>(),
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
    } {
        _ = e;
    }
}
impl<T: P2PMessage> ProtocolHandler for Protocol<T> {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (send, recv) = connection.accept_bi().await?;
        tokio::spawn(receive(
            PeerId::from(connection.remote_id()),
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
    mut peer_writer: MessageWriter<PeerJoined>,
    mut iroh: If<ResMut<IrohResource<T>>>,
    tokio: Res<TokioTasksRuntime>,
) {
    if let Err(e) = tokio.runtime().block_on(iroh.update(|peer| {
        peer_writer.write(PeerJoined::from(peer));
    })) {
        println!("{e:?}");
    }
    while let Ok((peer, message)) = iroh.messages.try_recv() {
        writer.write(MessageReceived { peer, message });
    }
}
