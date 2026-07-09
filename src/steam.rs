use bevy_ecs::resource::Resource;
use steamworks::{Client, SteamAPIInitError};
#[derive(Resource)]
pub struct SteamResource {
    pub client: Client,
}
impl SteamResource {
    pub fn new(id: u32) -> Result<Self, SteamAPIInitError> {
        let client = Client::init_app(id);
        client.map(|client| Self { client })
    }
}
