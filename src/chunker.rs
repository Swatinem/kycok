use std::pin::pin;

use fastcdc::v2020::AsyncStreamCDC;
use futures_util::{Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt};

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

#[async_trait::async_trait]
pub trait ChunkSink {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn push_chunk(&mut self, chunk: Vec<u8>) -> Result<(), Self::Error>;
}

pub use anyhow::Error;

/*async fn save_no_chunking(
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
}*/

pub fn chunk_stream(
    strategy: ChunkingStrategy,
    stream: impl AsyncRead + Unpin,
) -> impl Stream<Item = Result<Vec<u8>, Error>> {
    async_stream::try_stream! {
        let (min_size, avg_size, max_size) = match strategy {
            //ChunkingStrategy::None => return save_no_chunking(sink, stream).await,
            //ChunkingStrategy::Fixed(chunk_size) => return save_fixed(sink, stream, chunk_size).await,
            ChunkingStrategy::Cdc(min_size, avg_size, max_size) => (min_size, avg_size, max_size),
            _ => unimplemented!(),
        };

        let mut chunks = AsyncStreamCDC::new(stream, min_size, avg_size, max_size);
        let mut chunks = pin!(chunks.as_stream());

        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            yield chunk.data;
        }
    }
}

pub async fn chunk_stream_1<S: ChunkSink>(
    strategy: ChunkingStrategy,
    sink: &mut S,
    stream: impl AsyncRead + Unpin,
) -> Result<(), Error> {
    let (min_size, avg_size, max_size) = match strategy {
        //ChunkingStrategy::None => return save_no_chunking(sink, stream).await,
        //ChunkingStrategy::Fixed(chunk_size) => return save_fixed(sink, stream, chunk_size).await,
        ChunkingStrategy::Cdc(min_size, avg_size, max_size) => (min_size, avg_size, max_size),
        _ => unimplemented!(),
    };

    let mut chunks = AsyncStreamCDC::new(stream, min_size, avg_size, max_size);
    let mut chunks = pin!(chunks.as_stream());

    //let mut len = 0;
    //let mut file_hash = blake3::Hasher::new();
    //let mut stored_chunks = vec![];

    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?;

        sink.push_chunk(chunk.data).await?;

        /*let data = &chunk.data;
        let chunk_len = data.len() as u64;

        file_hash.update(data);
        len += chunk_len;

        let chunk_for_storing = Chunk {
            hash: blake3::hash(data),
            data: chunk.data,
        };
        let stored_chunk = self.backend.store_chunk(ctx, chunk_for_storing).await?;

        stored_chunks.push((chunk_len, stored_chunk));*/
    }

    Ok(())

    /*let file = File {
        hash: file_hash.finalize(),
        len,
        chunks: stored_chunks,
    };
    let file_id = self.backend.store_file(ctx, file).await?;
    Ok(file_id)*/
}
