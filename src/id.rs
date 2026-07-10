use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
#[derive(Clone, Copy)]
pub union PeerId {
    #[cfg(feature = "steam")]
    steam: steamworks::SteamId,
    #[cfg(feature = "iroh")]
    iroh: iroh::EndpointId,
}
impl PartialEq for PeerId {
    fn eq(&self, other: &Self) -> bool {
        cfg_select! {
            feature = "iroh" => self.iroh() == other.iroh(),
            feature = "steam" => self.steam() == other.steam(),
            _ => unreachable!(),
        }
    }
}
impl Eq for PeerId {}
impl Hash for PeerId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        cfg_select! {
            feature = "iroh" => self.iroh().hash(state),
            feature = "steam" => self.steam().hash(state),
            _ => unreachable!(),
        }
    }
}
#[cfg(feature = "iroh")]
impl From<iroh::EndpointId> for PeerId {
    fn from(iroh: iroh::EndpointId) -> Self {
        Self { iroh }
    }
}
#[cfg(feature = "steam")]
impl From<steamworks::SteamId> for PeerId {
    fn from(steam: steamworks::SteamId) -> Self {
        Self { steam }
    }
}
impl Debug for PeerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        cfg_select! {
            feature = "iroh" => Debug::fmt(&self.iroh(), f),
            feature = "steam" => Debug::fmt(&self.steam(), f),
            _ => unreachable!(),
        }
    }
}
impl PeerId {
    #[cfg(feature = "iroh")]
    pub fn iroh(self) -> iroh::EndpointId {
        unsafe { self.iroh }
    }
    #[cfg(feature = "steam")]
    pub fn steam(self) -> steamworks::SteamId {
        unsafe { self.steam }
    }
}
