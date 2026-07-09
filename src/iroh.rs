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
use bevy_tokio_tasks::tokio::sync::Mutex;
use bevy_tokio_tasks::tokio::sync::mpsc::{Receiver, Sender};
use bitcode::Buffer;
use iroh::endpoint::presets::N0;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointId};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
const ALPN: &[u8] = b"bevy_p2p";
#[derive(Resource)]
pub struct IrohResource<T: P2PMessage> {
    pub router: Router,
    pub protocol: Arc<Protocol<T>>,
    pub buffer: Buffer,
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
    mut tokio: ResMut<TokioTasksRuntime>,
    mut iroh: ResMut<IrohResource<T>>,
) {
    iroh.connect(&mut tokio, event.endpoint);
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
            let protocol = Arc::new(Protocol::default());
            let router = Router::builder(endpoint)
                .accept(ALPN, protocol.clone())
                .spawn();
            let buffer = Buffer::new();
            Self {
                router,
                protocol,
                buffer,
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
            let (tx, rx) = bevy_tokio_tasks::tokio::sync::mpsc::channel(256);
            bevy_tokio_tasks::tokio::spawn(receive(recv, tx));
            self.protocol
                .peers
                .lock()
                .await
                .push((connection, send, rx));
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
                for (_, send, _) in self.protocol.peers.lock().await.iter_mut() {
                    send.write_u32(bytes.len() as u32).await.unwrap();
                    send.write(bytes).await.unwrap();
                }
            }
        });
    }
    pub fn send<'a>(
        &mut self,
        runtime: &mut TokioTasksRuntime,
        msgs: impl Iterator<Item = (EndpointId, &'a T)>,
    ) {
        runtime.runtime().block_on(async {
            let mut peers = self.protocol.peers.lock().await;
            for (peer, msg) in msgs {
                if let Some((_, send, _)) = peers.iter_mut().find(|(c, _, _)| c.remote_id() == peer)
                {
                    let bytes = self.buffer.encode(msg);
                    send.write_u32(bytes.len() as u32).await.unwrap();
                    send.write(bytes).await.unwrap();
                }
            }
        });
    }
    pub fn receive(&mut self, runtime: &mut TokioTasksRuntime, mut f: impl FnMut(EndpointId, T)) {
        runtime.runtime().block_on(async {
            for (conn, _, recv) in self.protocol.peers.lock().await.iter_mut() {
                let peer = conn.remote_id();
                while let Ok(val) = recv.try_recv() {
                    f(peer, val);
                }
            }
        });
    }
}
pub struct Protocol<T: P2PMessage> {
    pub peers: Mutex<Vec<(Connection, SendStream, Receiver<T>)>>,
}
impl<T: P2PMessage> Debug for Protocol<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Protocol()")
    }
}
impl<T: P2PMessage> Default for Protocol<T> {
    fn default() -> Self {
        Self {
            peers: Mutex::new(Vec::with_capacity(256)),
        }
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
        let (tx, rx) = bevy_tokio_tasks::tokio::sync::mpsc::channel(256);
        bevy_tokio_tasks::tokio::spawn(receive(recv, tx));
        self.peers.lock().await.push((connection, send, rx));
        Ok(())
    }
}
pub(crate) fn message_to<T: P2PMessage>(
    mut tokio: ResMut<TokioTasksRuntime>,
    mut reader: PopulatedMessageReader<MessageTo<T>>,
    mut iroh: If<ResMut<IrohResource<T>>>,
) {
    iroh.send(
        &mut tokio,
        reader.read().map(|m| (m.peer_id.iroh(), &m.message)),
    )
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
    iroh.receive(&mut tokio, |peer, message| {
        writer.write(MessageReceived {
            peer: PeerId::from(peer),
            message,
        });
    });
}
