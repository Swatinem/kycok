pub use blake3::Hash;

#[derive(Debug)]
pub struct Chunk {
    pub hash: Hash,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct File<C> {
    pub hash: Hash,
    pub len: u64,
    pub chunks: Vec<(u64, C)>,
}

#[async_trait::async_trait]
pub trait Backend {
    type Context;
    type StoredChunk;
    type FileId;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn store_chunk(
        &self,
        ctx: &Self::Context,
        chunk: Chunk,
    ) -> Result<Self::StoredChunk, Self::Error>;

    async fn store_file(
        &self,
        ctx: &Self::Context,
        file: File<Self::StoredChunk>,
    ) -> Result<Self::FileId, Self::Error>;
}
