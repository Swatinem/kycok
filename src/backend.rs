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
    type FileId;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn push_chunk(&mut self, chunk: Vec<u8>) -> Result<(), Self::Error>;

    async fn finish() -> Result<(), Self::Error>;
}
