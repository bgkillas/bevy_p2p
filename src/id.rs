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
    #[cfg(any(not(feature = "steam"), feature = "iroh"))]
    fn eq(&self, other: &Self) -> bool {
        self.iroh() == other.iroh()
    }
    #[cfg(all(feature = "steam", not(feature = "iroh")))]
    fn eq(&self, other: &Self) -> bool {
        self.steam() == other.steam()
    }
    #[cfg(not(any(feature = "steam", feature = "iroh")))]
    fn eq(&self, other: &Self) -> bool {
        unreachable!()
    }
}
impl Eq for PeerId {}
impl Hash for PeerId {
    #[cfg(any(not(feature = "steam"), feature = "iroh"))]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.iroh().hash(state);
    }
    #[cfg(all(feature = "steam", not(feature = "iroh")))]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.steam().hash(state);
    }
    #[cfg(not(any(feature = "steam", feature = "iroh")))]
    fn hash<H: Hasher>(&self, state: &mut H) {
        unreachable!()
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
    #[cfg(any(not(feature = "steam"), feature = "iroh"))]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.iroh())
    }
    #[cfg(all(feature = "steam", not(feature = "iroh")))]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.steam())
    }
    #[cfg(not(any(feature = "steam", feature = "iroh")))]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unreachable!()
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
