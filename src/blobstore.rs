use tokio::io::AsyncRead;

#[derive(Debug, Clone, Copy)]
pub struct BlobId([u8; 16]);

#[async_trait::async_trait]
pub trait BlobStore {
    type Error: std::error::Error + Send + Sync + 'static;
    type FetchRead: AsyncRead;

    async fn allocate_id(&self) -> Result<BlobId, Self::Error>;

    async fn store_blob(&self, id: BlobId, blob: Vec<u8>) -> Result<(), Self::Error>;

    async fn fetch_blob(&self, id: BlobId) -> Result<Self::FetchRead, Self::Error>;

    async fn delete_blob(&self, id: BlobId) -> Result<(), Self::Error>;
}
