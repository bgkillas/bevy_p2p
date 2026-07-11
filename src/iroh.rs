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
use iroh::endpoint::{BindError, Connection, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use rustc_hash::{FxBuildHasher, FxHashMap};
use std::fmt::{Debug, Formatter};
use std::io;
use std::sync::Arc;
const ALPN: &[u8] = b"bevy_p2p";
#[derive(Resource)]
pub struct IrohResource<T: P2PMessage> {
    pub router: Router,
    pub connections: FxHashMap<PeerId, (Connection, SendStream)>,
    pub my_id: PeerId,
    buffer: Buffer,
    messages: Receiver<(PeerId, T)>,
    messages_send: Arc<Sender<(PeerId, T)>>,
    new_peers: Receiver<(Connection, SendStream)>,
    peer_relay: Receiver<Box<[PeerId]>>,
    peer_relay_send: Arc<Sender<Box<[PeerId]>>>,
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
        if let Err(e) = try {
            if let Some(mut iroh) = iroh_opt {
                iroh.connect(event.peer).await?;
            } else {
                let mut iroh = IrohResource::<T>::bind().await.unwrap();
                iroh.connect(event.peer).await?;
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
        let peer_relay_send = Arc::new(peer_tx);
        let router = Router::builder(endpoint)
            .accept(
                ALPN,
                Protocol::new(new_tx, messages_send.clone(), peer_relay_send.clone()),
            )
            .spawn();
        let buffer = Buffer::new();
        let connections = FxHashMap::with_capacity_and_hasher(8, FxBuildHasher);
        Ok(Self {
            my_id,
            router,
            buffer,
            connections,
            new_peers,
            messages,
            messages_send,
            peer_relay,
            peer_relay_send,
        })
    }
    pub async fn connect(&mut self, peer: PeerId) -> Result<(), io::Error> {
        if self.connections.contains_key(&peer) {
            return Ok(());
        }
        let connection = self
            .router
            .endpoint()
            .connect(peer.iroh(), ALPN)
            .await
            .unwrap();
        let (mut send, recv) = connection.open_bi().await?;
        self.relay_peer(&mut send).await?;
        let peer = PeerId::from(connection.remote_id());
        bevy_tokio_tasks::tokio::spawn(receive(
            peer,
            recv,
            self.messages_send.clone(),
            self.peer_relay_send.clone(),
        ));
        self.connections.insert(peer, (connection, send));
        Ok(())
    }
    pub async fn relay_peer(&mut self, send: &mut SendStream) -> Result<(), io::Error> {
        let len = u32::try_from(self.connections.len()).unwrap();
        send.write_u32(len).await?;
        for peer in self.connections.keys() {
            send.write_all(peer.iroh().as_bytes()).await?;
        }
        Ok(())
    }
    pub async fn update(&mut self) -> Result<(), io::Error> {
        while let Ok((connection, mut send)) = self.new_peers.try_recv() {
            self.relay_peer(&mut send).await?;
            let peer = PeerId::from(connection.remote_id());
            self.connections.insert(peer, (connection, send));
        }
        while let Ok(peers) = self.peer_relay.try_recv() {
            for peer in peers {
                self.connect(peer).await?;
            }
        }
        Ok(())
    }
    pub async fn broadcast(&mut self, msg: T) -> Result<(), io::Error> {
        let bytes = self.buffer.encode(&msg);
        let len = u32::try_from(bytes.len()).unwrap();
        for (_, send) in self.connections.values_mut() {
            send.write_u32(len).await?;
            send.write_all(bytes).await?;
        }
        Ok(())
    }
    pub async fn send(&mut self, peer: PeerId, msg: T) -> Result<(), io::Error> {
        if let Some((_, send)) = self.connections.get_mut(&peer) {
            let bytes = self.buffer.encode(&msg);
            let len = u32::try_from(bytes.len()).unwrap();
            send.write_u32(len).await?;
            send.write_all(bytes).await?;
        }
        Ok(())
    }
}
struct Protocol<T: P2PMessage> {
    pub sender: Sender<(Connection, SendStream)>,
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
        sender: Sender<(Connection, SendStream)>,
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
async fn receive<T: P2PMessage>(
    peer: PeerId,
    mut recv: RecvStream,
    send: Arc<Sender<(PeerId, T)>>,
    peer_relay: Arc<Sender<Box<[PeerId]>>>,
) {
    let Ok(size) = recv.read_u32().await else {
        unreachable!();
    };
    if size != 0 {
        let len = size as usize;
        let mut peers_buf = vec![0; len * size_of::<PeerId>()];
        recv.read_exact(&mut peers_buf).await.unwrap();
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
    while let Ok(size) = recv.read_u32().await {
        let len = size as usize;
        if len > recv_buffer.len() {
            recv_buffer.resize(len, 0);
        }
        recv.read_exact(&mut recv_buffer[..len]).await.unwrap();
        let val = buffer.decode(&recv_buffer[..len]).unwrap();
        send.send((peer, val)).await.unwrap();
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
    if let Err(e) = tokio.runtime().block_on(iroh.update()) {
        println!("{e:?}")
    }
    while let Ok((peer, message)) = iroh.messages.try_recv() {
        writer.write(MessageReceived { peer, message });
    }
}
