use crate::id::PeerId;
use crate::iroh::IrohResource;
use bevy_ecs::message::Message;
use bevy_ecs::system::{Res, ResMut, SystemParam};
use bevy_tokio_tasks::TokioTasksRuntime;
use bitcode::{DecodeOwned, Encode};
#[derive(SystemParam)]
pub struct Net<'w, T: P2PMessage> {
    pub iroh: Option<ResMut<'w, IrohResource<T>>>,
    pub tokio: Res<'w, TokioTasksRuntime>,
}
impl<T: P2PMessage> Net<'_, T> {
    pub fn send(&mut self, peer: PeerId, message: T) {
        if let Some(iroh) = &mut self.iroh {
            self.tokio.runtime().block_on(async {
                iroh.send(peer, message).await;
            });
        }
    }
    pub fn broadcast(&mut self, message: T) {
        if let Some(iroh) = &mut self.iroh {
            self.tokio.runtime().block_on(async {
                iroh.broadcast(message).await;
            });
        }
    }
}
#[derive(Message)]
pub struct MessageReceived<T: P2PMessage> {
    pub peer: PeerId,
    pub message: T,
}
pub trait P2PMessage: Send + Sync + Encode + DecodeOwned + 'static {}
impl<T: Send + Sync + Encode + DecodeOwned + 'static> P2PMessage for T {}
