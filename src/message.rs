#![allow(clippy::shadow_reuse)]
use crate::iroh_res::IrohResource;
use crate::runtime::Runtime;
use bevy_ecs::message::{Message, MessageWriter};
use bevy_ecs::system::{Res, SystemParam};
use bitcode::{DecodeOwned, Encode};
use iroh::EndpointId;
#[derive(SystemParam)]
pub struct Net<'w, 's, T: P2PMessage> {
    pub iroh: Option<Res<'w, IrohResource<T>>>,
    pub runtime: Runtime<'w, 's>,
    pub disconnect: MessageWriter<'w, PeerDisconnected>,
}
impl<T: P2PMessage> Net<'_, '_, T> {
    pub fn send(&mut self, peer: EndpointId, message: T) {
        if let Some(ir) = &self.iroh {
            let iroh = ir.inner.clone();
            self.runtime.spawn_loose(async move {
                iroh.lock().await.send(peer, &message).await;
            });
        }
    }
    pub fn broadcast(&mut self, message: T) {
        if let Some(ir) = &self.iroh {
            let iroh = ir.inner.clone();
            self.runtime.spawn_loose(async move {
                iroh.lock().await.broadcast(&message).await;
            });
        }
    }
}
#[derive(Message)]
pub struct ConnectFailed {
    pub peer: EndpointId,
}
impl From<EndpointId> for ConnectFailed {
    fn from(peer: EndpointId) -> Self {
        Self { peer }
    }
}
#[derive(Message)]
pub struct PeerConnected {
    pub peer: EndpointId,
}
impl From<EndpointId> for PeerConnected {
    fn from(peer: EndpointId) -> Self {
        Self { peer }
    }
}
#[derive(Message)]
pub struct PeerDisconnected {
    pub peer: EndpointId,
}
impl From<EndpointId> for PeerDisconnected {
    fn from(peer: EndpointId) -> Self {
        Self { peer }
    }
}
#[derive(Message)]
pub struct MessageReceived<T: P2PMessage> {
    pub peer: EndpointId,
    pub message: T,
}
pub trait P2PMessage: Send + Sync + Encode + DecodeOwned + 'static {}
impl<T: Send + Sync + Encode + DecodeOwned + 'static> P2PMessage for T {}
