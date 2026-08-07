#![allow(clippy::shadow_reuse)]
use crate::iroh_res::IrohResource;
use crate::runtime::Runtime;
use bevy_ecs::message::Message;
use bevy_ecs::system::{Res, SystemParam};
use bitcode::{DecodeOwned, Encode};
use iroh::EndpointId;
#[derive(SystemParam)]
pub struct Net<'w, T: P2PMessage> {
    pub iroh: Option<Res<'w, IrohResource<T>>>,
    pub runtime: Res<'w, Runtime>,
}
impl<T: P2PMessage> Net<'_, T> {
    pub fn send(&self, peer: EndpointId, message: T) {
        if let Some(ir) = &self.iroh {
            let iroh = ir.inner.clone();
            self.runtime.spawn(async move {
                iroh.lock().await.send(peer, &message).await;
            });
        }
    }
    pub fn broadcast(&self, message: T) {
        if let Some(ir) = &self.iroh {
            let iroh = ir.inner.clone();
            self.runtime.spawn(async move {
                iroh.lock().await.broadcast(&message).await;
            });
        }
    }
}
#[derive(Message)]
pub struct MessageReceived<T: P2PMessage> {
    pub peer: EndpointId,
    pub message: T,
}
pub trait P2PMessage: Send + Sync + Encode + DecodeOwned + 'static {}
impl<T: Send + Sync + Encode + DecodeOwned + 'static> P2PMessage for T {}
