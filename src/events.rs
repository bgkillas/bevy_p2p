use bevy_ecs::event::Event;
use iroh::EndpointId;
#[derive(Event)]
pub struct Binded;
#[derive(Event)]
pub struct PeerConnected {
    pub peer: EndpointId,
}
#[derive(Event)]
pub struct PeerDisconnected {
    pub peer: EndpointId,
}
#[derive(Event)]
pub struct ConnectFailed {
    pub peer: EndpointId,
}
