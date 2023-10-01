use std::pin::pin;

use fastcdc::v2020::AsyncStreamCDC;
use futures_util::StreamExt;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::backend::{Backend, Chunk, File};

pub enum ChunkingStrategy {
    None,
    Fixed(u32),
    Cdc(u32, u32, u32),
}

const ONE_MEG: u32 = 1024 * 1024;
#[allow(clippy::identity_op)]
const MIN_CHUNK: u32 = 1 * ONE_MEG;
const AVG_CHUNK: u32 = 2 * ONE_MEG;
const MAX_CHUNK: u32 = 4 * ONE_MEG;

impl Default for ChunkingStrategy {
    fn default() -> Self {
        Self::Cdc(MIN_CHUNK, AVG_CHUNK, MAX_CHUNK)
    }
}

pub struct FileConfig {
    pub chunking_strategy: ChunkingStrategy,
}

pub struct Chunker<B> {
    backend: B,
}

pub use anyhow::Error; // TODO: maybe use a dedicated error type?

impl<B: Backend> Chunker<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    async fn save_no_chunking(
        &self,
        ctx: &B::Context,
        mut stream: impl AsyncRead + Unpin,
    ) -> Result<B::FileId, Error> {
        let mut data = vec![];
        // FIXME: we want to have a hash first to provide to the backend, so we
        // have to read everything into an in-memory buffer first
        let len = stream.read_to_end(&mut data).await? as u64;
        let hash = blake3::hash(&data);
        let chunk_for_storing = Chunk { hash, data };
        let stored_chunk = self.backend.store_chunk(ctx, chunk_for_storing).await?;

        let file = File {
            hash,
            len,
            chunks: vec![(len, stored_chunk)],
        };
        let file_id = self.backend.store_file(ctx, file).await?;
        Ok(file_id)
    }

    async fn save_fixed(
        &self,
        ctx: &B::Context,
        mut stream: impl AsyncRead + Unpin,
        chunk_size: u32,
    ) -> Result<B::FileId, Error> {
        let mut len = 0;
        let mut file_hash = blake3::Hasher::new();
        let mut stored_chunks = vec![];

        loop {
            let mut steam_chunk = (&mut stream).take(chunk_size as u64);

            let mut data = Vec::with_capacity(chunk_size as usize);
            let chunk_len = steam_chunk.read_to_end(&mut data).await? as u64;
            let hash = blake3::hash(&data);

            file_hash.update(&data);
            len += chunk_len as u64;

            let chunk_for_storing = Chunk { hash, data };
            let stored_chunk = self.backend.store_chunk(ctx, chunk_for_storing).await?;
            stored_chunks.push((chunk_len, stored_chunk));

            if chunk_len < chunk_size as u64 {
                break;
            }
        }

        let file = File {
            hash: file_hash.finalize(),
            len,
            chunks: stored_chunks,
        };
        let file_id = self.backend.store_file(ctx, file).await?;
        Ok(file_id)
    }

    pub async fn save_file(
        &self,
        config: &FileConfig,
        ctx: &B::Context,
        stream: impl AsyncRead + Unpin,
    ) -> Result<B::FileId, Error> {
        let (min_size, avg_size, max_size) = match config.chunking_strategy {
            ChunkingStrategy::None => return self.save_no_chunking(ctx, stream).await,
            ChunkingStrategy::Fixed(chunk_size) => {
                return self.save_fixed(ctx, stream, chunk_size).await
            }
            ChunkingStrategy::Cdc(min_size, avg_size, max_size) => (min_size, avg_size, max_size),
        };

        let mut chunks = AsyncStreamCDC::new(stream, min_size, avg_size, max_size);
        let mut chunks = pin!(chunks.as_stream());

        let mut len = 0;
        let mut file_hash = blake3::Hasher::new();
        let mut stored_chunks = vec![];

        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            let data = &chunk.data;
            let chunk_len = data.len() as u64;

            file_hash.update(data);
            len += chunk_len;

            let chunk_for_storing = Chunk {
                hash: blake3::hash(data),
                data: chunk.data,
            };
            let stored_chunk = self.backend.store_chunk(ctx, chunk_for_storing).await?;

            stored_chunks.push((chunk_len, stored_chunk));
        }

        let file = File {
            hash: file_hash.finalize(),
            len,
            chunks: stored_chunks,
        };
        let file_id = self.backend.store_file(ctx, file).await?;
        Ok(file_id)
    }
}
