#![allow(clippy::shadow_reuse)]
use crate::id::PeerId;
use crate::iroh::IrohResource;
use bevy_ecs::message::{Message, MessageWriter};
use bevy_ecs::system::{Res, ResMut, SystemParam};
use bevy_tokio_tasks::TokioTasksRuntime;
use bitcode::{DecodeOwned, Encode};
#[derive(SystemParam)]
pub struct Net<'w, T: P2PMessage> {
    pub iroh: Option<ResMut<'w, IrohResource<T>>>,
    pub tokio: Res<'w, TokioTasksRuntime>,
    pub disconnect: MessageWriter<'w, PeerDisconnected>,
}
impl<T: P2PMessage> Net<'_, T> {
    pub fn send(&mut self, peer: PeerId, message: &T) {
        if let Some(ir) = &mut self.iroh {
            self.tokio
                .runtime()
                .block_on(ir.send(peer, message, |peer| {
                    self.disconnect.write(PeerDisconnected::from(peer));
                }));
        }
    }
    pub fn broadcast(&mut self, message: &T) {
        if let Some(ir) = &mut self.iroh {
            self.tokio.runtime().block_on(ir.broadcast(message, |peer| {
                self.disconnect.write(PeerDisconnected::from(peer));
            }));
        }
    }
}
#[derive(Message)]
pub struct ConnectFailed {
    pub peer: PeerId,
}
impl From<PeerId> for ConnectFailed {
    fn from(peer: PeerId) -> Self {
        Self { peer }
    }
}
#[derive(Message)]
pub struct PeerConnected {
    pub peer: PeerId,
}
impl From<PeerId> for PeerConnected {
    fn from(peer: PeerId) -> Self {
        Self { peer }
    }
}
#[derive(Message)]
pub struct PeerDisconnected {
    pub peer: PeerId,
}
impl From<PeerId> for PeerDisconnected {
    fn from(peer: PeerId) -> Self {
        Self { peer }
    }
}
#[derive(Message)]
pub struct MessageReceived<T: P2PMessage> {
    pub peer: PeerId,
    pub message: T,
}
pub trait P2PMessage: Send + Sync + Encode + DecodeOwned + 'static {}
impl<T: Send + Sync + Encode + DecodeOwned + 'static> P2PMessage for T {}
