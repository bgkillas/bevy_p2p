use std::fmt::{Debug, Formatter};
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
        true
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
        let bytes =
            unsafe { std::mem::transmute_copy::<[u8; 32], [u64; 4]>(self.iroh().as_bytes()) };
        write!(
            f,
            "{:x}-{:x}-{:x}-{:x}",
            bytes[0], bytes[1], bytes[2], bytes[3]
        )
    }
    #[cfg(all(feature = "steam", not(feature = "iroh")))]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}", self.steam())
    }
    #[cfg(not(any(feature = "steam", feature = "iroh")))]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
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
