use crate::id::PeerId;
use crate::message::{MessageBroadcast, MessageReceived, MessageTo, P2PMessage};
use bevy::prelude::Resource;
use bevy_ecs::event::Event;
use bevy_ecs::message::{MessageWriter, PopulatedMessageReader};
use bevy_ecs::observer::On;
use bevy_ecs::system::{Commands, If, ResMut};
use bevy_tokio_tasks::TokioTasksRuntime;
use bevy_tokio_tasks::tokio::io::{AsyncReadExt, AsyncWriteExt};
use bitcode::Buffer;
use iroh::endpoint::presets::N0;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use std::sync::{Arc, Mutex};
const ALPN: &[u8] = b"bevy_p2p";
#[derive(Resource)]
pub struct IrohResource {
    pub router: Router,
    pub protocol: Arc<Protocol>,
    pub buffer: Buffer,
    pub recv_buffer: Vec<u8>,
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
pub(crate) fn on_connect(
    event: On<IrohConnect>,
    mut tokio: ResMut<TokioTasksRuntime>,
    mut iroh: ResMut<IrohResource>,
) {
    iroh.connect(&mut tokio, EndpointAddr::from(event.endpoint));
}
#[derive(Event)]
pub struct IrohBind;
pub(crate) fn on_bind(
    _: On<IrohBind>,
    mut commands: Commands,
    mut tokio: ResMut<TokioTasksRuntime>,
) {
    let iroh = IrohResource::bind(&mut tokio);
    commands.insert_resource(iroh);
}
impl IrohResource {
    pub fn bind(runtime: &mut TokioTasksRuntime) -> Self {
        runtime.runtime().block_on(async {
            let endpoint = Endpoint::bind(N0).await.unwrap();
            let protocol = Arc::new(Protocol::default());
            let router = Router::builder(endpoint)
                .accept(ALPN, protocol.clone())
                .spawn();
            let buffer = Buffer::new();
            let recv_buffer = Vec::new();
            Self {
                router,
                protocol,
                buffer,
                recv_buffer,
            }
        })
    }
    pub fn connect(&mut self, runtime: &mut TokioTasksRuntime, addr: EndpointAddr) {
        runtime.runtime().block_on(async {
            let connection = self.router.endpoint().connect(addr, ALPN).await.unwrap();
            let (send, recv) = connection.open_bi().await.unwrap();
            self.protocol
                .peers
                .lock()
                .unwrap()
                .push((connection, send, recv));
        });
    }
    pub fn broadcast<'a, T: P2PMessage>(
        &mut self,
        runtime: &mut TokioTasksRuntime,
        msgs: impl Iterator<Item = &'a T>,
    ) {
        runtime.runtime().block_on(async {
            for msg in msgs {
                let bytes = self.buffer.encode(msg);
                for (_, send, _) in self.protocol.peers.lock().unwrap().iter_mut() {
                    send.write_u32(bytes.len() as u32).await.unwrap();
                    send.write(&bytes).await.unwrap();
                    send.finish().unwrap();
                }
            }
        });
    }
    pub fn send<'a, T: P2PMessage>(
        &mut self,
        runtime: &mut TokioTasksRuntime,
        msgs: impl Iterator<Item = (EndpointId, &'a T)>,
    ) {
        runtime.runtime().block_on(async {
            let mut peers = self.protocol.peers.lock().unwrap();
            for (peer, msg) in msgs {
                if let Some((_, send, _)) = peers.iter_mut().find(|(c, _, _)| c.remote_id() == peer)
                {
                    let bytes = self.buffer.encode(msg);
                    send.write_u32(bytes.len() as u32).await.unwrap();
                    send.write(&bytes).await.unwrap();
                    send.finish().unwrap();
                }
            }
        });
    }
    pub fn receive<T: P2PMessage>(
        &mut self,
        runtime: &mut TokioTasksRuntime,
        mut f: impl FnMut(EndpointId, T),
    ) {
        runtime.runtime().block_on(async {
            for (conn, _, recv) in self.protocol.peers.lock().unwrap().iter_mut() {
                let peer = conn.remote_id();
                while let Ok(size) = recv.read_u32().await {
                    let len = size as usize;
                    if len > self.recv_buffer.len() {
                        self.recv_buffer.resize(len, 0);
                    }
                    recv.read_exact(&mut self.recv_buffer[..len]).await.unwrap();
                    let val = self.buffer.decode(&self.recv_buffer[..len]).unwrap();
                    f(peer, val);
                }
            }
        });
    }
}
#[derive(Debug)]
pub struct Protocol {
    pub peers: Mutex<Vec<(Connection, SendStream, RecvStream)>>,
}
impl Default for Protocol {
    fn default() -> Self {
        Self {
            peers: Mutex::new(Vec::with_capacity(256)),
        }
    }
}
impl ProtocolHandler for Protocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (send, recv) = connection.accept_bi().await?;
        self.peers.lock().unwrap().push((connection, send, recv));
        Ok(())
    }
}
pub(crate) fn message_to<T: P2PMessage>(
    mut tokio: ResMut<TokioTasksRuntime>,
    mut reader: PopulatedMessageReader<MessageTo<T>>,
    mut iroh: If<ResMut<IrohResource>>,
) {
    iroh.send(
        &mut tokio,
        reader.read().map(|m| (m.peer_id.iroh(), &m.message)),
    )
}
pub(crate) fn message_broadcast<T: P2PMessage>(
    mut tokio: ResMut<TokioTasksRuntime>,
    mut reader: PopulatedMessageReader<MessageBroadcast<T>>,
    mut iroh: If<ResMut<IrohResource>>,
) {
    iroh.broadcast(&mut tokio, reader.read().map(|m| &m.message))
}
pub(crate) fn receive_messages<T: P2PMessage>(
    mut tokio: ResMut<TokioTasksRuntime>,
    mut writer: MessageWriter<MessageReceived<T>>,
    mut iroh: If<ResMut<IrohResource>>,
) {
    iroh.receive(&mut tokio, |peer, message| {
        writer.write(MessageReceived {
            peer: PeerId::from(peer),
            message,
        });
    });
}
