use crate::id::PeerId;
use bevy_ecs::message::{Message, MessageWriter};
use bevy_ecs::system::SystemParam;
use bitcode::{DecodeOwned, Encode};
#[derive(Message)]
pub struct MessageTo<T: P2PMessage> {
    pub peer_id: PeerId,
    pub message: T,
}
#[derive(Message)]
pub struct MessageBroadcast<T: P2PMessage> {
    pub message: T,
}
#[derive(SystemParam)]
pub struct Net<'w, T: P2PMessage> {
    pub message_to: MessageWriter<'w, MessageTo<T>>,
    pub message_broadcast: MessageWriter<'w, MessageBroadcast<T>>,
}
impl<T: P2PMessage> Net<'_, T> {
    pub fn send(&mut self, peer_id: PeerId, message: T) {
        self.message_to.write(MessageTo { peer_id, message });
    }
    pub fn broadcast(&mut self, message: T) {
        self.message_broadcast.write(MessageBroadcast { message });
    }
}
#[derive(Message)]
pub struct MessageReceived<T: P2PMessage> {
    pub peer: PeerId,
    pub message: T,
}
pub trait P2PMessage: Send + Sync + Encode + DecodeOwned + 'static {}
impl<T: Send + Sync + Encode + DecodeOwned + 'static> P2PMessage for T {}
