use crate::message::{MessageBroadcast, MessageReceived, MessageTo, P2PMessage};
use bevy_app::{App, FixedUpdate, Plugin};
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
        app.add_message::<MessageTo<T>>();
        app.add_message::<MessageBroadcast<T>>();
        app.add_message::<MessageReceived<T>>();
        #[cfg(feature = "iroh")]
        {
            app.add_systems(
                FixedUpdate,
                (
                    crate::iroh::message_to::<T>,
                    crate::iroh::message_broadcast::<T>,
                    crate::iroh::receive_messages::<T>,
                ),
            );
            app.add_observer(crate::iroh::on_bind);
            app.add_observer(crate::iroh::on_connect);
        }
    }
}
