#[derive(Debug)]
#[repr(u8)]
pub enum MetadataVersion {
    V0 = 0,
}

#[derive(Debug)]
#[repr(u8)]
pub enum ChunkType {
    Chunk = 0,
    SingleChunkFile = 1,
    MultiChunkFile = 2,
}

#[derive(Debug)]
#[repr(u8)]
pub enum HashAlgorithm {
    Blake3 = 0,
}

#[derive(Debug)]
#[repr(u8)]
pub enum Compression {
    None = 0,
    Zstd = 1,
}

#[derive(Debug)]
pub struct Metadata {
    pub version: MetadataVersion,
    pub chunk_type: ChunkType,
    pub hash_algorithm: HashAlgorithm,
    pub compression: Compression,
    pub hash: [u8; 32],
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            version: MetadataVersion::V0,
            chunk_type: ChunkType::Chunk,
            hash_algorithm: HashAlgorithm::Blake3,
            compression: Compression::None,
            hash: Default::default(),
        }
    }
}
