use bitcode::{Decode, Encode};
use iroh::EndpointId;
#[derive(Encode, Decode)]
#[repr(transparent)]
pub struct DataCoderBoxId {
    pub data: Box<[[u8; 32]]>,
}
impl From<&Box<[EndpointId]>> for DataCoderBoxId {
    fn from(value: &Box<[EndpointId]>) -> Self {
        Self {
            data: value.iter().map(|v| *v.as_bytes()).collect(),
        }
    }
}
impl From<DataCoderBoxId> for Box<[EndpointId]> {
    fn from(value: DataCoderBoxId) -> Self {
        value
            .data
            .into_iter()
            .map(|v| EndpointId::from_bytes(&v).unwrap())
            .collect()
    }
}
