use crate::id::PeerId;
use crate::message::{MessageBroadcast, MessageReceived, MessageTo, P2PMessage};
use bevy::prelude::Resource;
use bevy_ecs::event::Event;
use bevy_ecs::message::{MessageWriter, PopulatedMessageReader};
use bevy_ecs::observer::On;
use bevy_ecs::system::{Commands, If, ResMut};
use bevy_tokio_tasks::TokioTasksRuntime;
use iroh::endpoint::presets::N0;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr};
use std::sync::{Arc, Mutex};
const ALPN: &[u8] = b"bevy_p2p";
#[derive(Resource)]
pub struct IrohResource {
    pub router: Router,
    pub protocol: Arc<Protocol>,
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
            Self { router, protocol }
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
        &self,
        runtime: &mut TokioTasksRuntime,
        msgs: impl Iterator<Item = &'a T>,
    ) {
        runtime.runtime().block_on(async {
            for msg in msgs {
                let bytes = bitcode::encode(msg);
                for (_, send, _) in self.protocol.peers.lock().unwrap().iter_mut() {
                    send.write(&bytes).await.unwrap();
                }
            }
        });
    }
    pub fn receive<T: P2PMessage>(
        &self,
        runtime: &mut TokioTasksRuntime,
        mut f: impl FnMut(PeerId, T),
    ) {
        runtime.runtime().block_on(async {
            for (_, _, recv) in self.protocol.peers.lock().unwrap().iter_mut() {
                let peer = PeerId { id: 0 };
                let bytes = &[];
                let val = bitcode::decode(bytes).unwrap();
                f(peer, val);
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
    iroh: If<ResMut<IrohResource>>,
) {
}
pub(crate) fn message_broadcast<T: P2PMessage>(
    mut tokio: ResMut<TokioTasksRuntime>,
    mut reader: PopulatedMessageReader<MessageBroadcast<T>>,
    iroh: If<ResMut<IrohResource>>,
) {
    iroh.broadcast(&mut tokio, reader.read().map(|m| &m.message))
}
pub(crate) fn receive_messages<T: P2PMessage>(
    mut tokio: ResMut<TokioTasksRuntime>,
    mut writer: MessageWriter<MessageReceived<T>>,
    iroh: If<ResMut<IrohResource>>,
) {
    iroh.receive(&mut tokio, |peer, message| {
        writer.write(MessageReceived { peer, message });
    });
}
