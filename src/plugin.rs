use crate::message::{MessageReceived, P2PMessage, PeerConnected, PeerDisconnected};
use bevy_app::{App, FixedPreUpdate, Plugin};
use std::marker::PhantomData;
#[cfg(feature = "steam")]
pub struct P2PPlugin<T: P2PMessage> {
    pub id: u32,
    phantom: PhantomData<T>,
}
#[cfg(not(feature = "steam"))]
pub struct P2PPlugin<T: P2PMessage> {
    phantom: PhantomData<T>,
}
impl<T: P2PMessage> P2PPlugin<T> {
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub fn new(#[cfg(feature = "steam")] id: u32) -> Self {
        Self {
            #[cfg(feature = "steam")]
            id,
            phantom: PhantomData,
        }
    }
}
impl<T: P2PMessage> Plugin for P2PPlugin<T> {
    fn build(&self, app: &mut App) {
        #[cfg(feature = "steam")]
        match crate::steam::SteamResource::new(self.id) {
            Ok(res) => {
                app.insert_resource(res);
            }
            Err(err) => bevy_log::warn!("{err}"),
        }
        app.add_message::<PeerConnected>();
        app.add_message::<PeerDisconnected>();
        app.add_message::<MessageReceived<T>>();
        #[cfg(feature = "iroh")]
        {
            app.add_systems(FixedPreUpdate, crate::iroh::receive_messages::<T>);
            app.add_observer(crate::iroh::on_bind::<T>);
            app.add_observer(crate::iroh::on_unbind::<T>);
            app.add_observer(crate::iroh::on_connect::<T>);
        }
    }
}
