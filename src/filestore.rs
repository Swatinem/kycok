use std::pin::pin;

use futures_util::{Stream, StreamExt};
use tokio::io::AsyncRead;

use crate::blobstore::{BlobId, BlobStore};
use crate::chunker::{chunk_stream, ChunkingStrategy};
use crate::metastore::{ChunkType, Compression, Metadata};

pub struct MetaId([u8; 32]);

#[derive(Debug)]
pub struct FileStore<B> {
    blobstore: B,
}

pub use anyhow::Error;

struct StoredChunk {
    chunk_len: usize,
    chunk_meta: Metadata,
    blob_id: BlobId,
}

impl<B: BlobStore> FileStore<B> {
    pub fn new(blobstore: B) -> Self {
        Self { blobstore }
    }

    pub async fn store_file(&self, stream: impl AsyncRead + Unpin) -> Result<Metadata, Error> {
        let strategy = ChunkingStrategy::default();
        let mut chunks = pin!(chunk_stream(strategy, stream));

        let mut file_hash = blake3::Hasher::new();
        let mut stored_chunks = vec![];

        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            let chunk_len = chunk.len();

            file_hash.update(&chunk);

            let chunk_hash = blake3::hash(&chunk);

            let chunk_meta = Metadata {
                compression: Compression::Zstd,
                hash: *chunk_hash.as_bytes(),
                ..Default::default()
            };
            let blob_id = self.store_blob(&chunk_meta, chunk).await?;

            stored_chunks.push(StoredChunk {
                chunk_len,
                chunk_meta,
                blob_id,
            });
        }
        let file_hash = file_hash.finalize();
        let file_hash = *file_hash.as_bytes();

        let (metadata, blob_id) = if stored_chunks.len() == 1 {
            (
                Metadata {
                    chunk_type: ChunkType::SingleChunkFile,
                    hash: file_hash, // assert: file_hash == chunk_hash?
                    ..Default::default()
                },
                stored_chunks.last().unwrap().blob_id,
            )
        } else {
            let metadata = Metadata {
                chunk_type: ChunkType::MultiChunkFile,
                hash: file_hash,
                ..Default::default()
            };

            let blob = create_chunk_index(stored_chunks);
            let blob_id = self.store_blob(&metadata, blob).await?;

            (metadata, blob_id)
        };

        Ok(metadata)
    }

    async fn store_blob(&self, metadata: &Metadata, blob: Vec<u8>) -> Result<BlobId, Error> {
        let blob_id = self.blobstore.allocate_id().await?;

        self.blobstore.store_blob(blob_id, blob).await?;

        Ok(blob_id)
    }
}

fn create_chunk_index(stored_chunks: Vec<StoredChunk>) -> Vec<u8> {
    vec![]
}
