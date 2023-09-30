use std::net::SocketAddr;

use axum::extract::BodyStream;
use axum::routing::post;
use axum::Router;
use futures_util::StreamExt;
use kycok::backend::{Backend, Chunk, File};
use kycok::chunker::{Chunker, FileConfig};
use tokio_util::io::StreamReader;

#[tokio::main]
async fn main() {
    let app = Router::new().route("/", post(upload_stuff));

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

// basic handler that responds with a static string
async fn upload_stuff(body: BodyStream) -> &'static str {
    let stream = body
        .map(|result| result.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err)));
    let read = StreamReader::new(stream);

    let chunker = Chunker::new(PrintBackend);
    let config = FileConfig {
        chunking_strategy: Default::default(),
    };
    chunker.save_file(&config, &(), read).await.unwrap();

    "OK"
}

struct PrintBackend;

#[async_trait::async_trait]
impl Backend for PrintBackend {
    type Context = ();
    type StoredChunk = u64;
    type FileId = ();
    type Error = std::io::Error;

    async fn store_chunk(&self, _ctx: &Self::Context, chunk: Chunk) -> Result<u64, Self::Error> {
        let data = &chunk.data;
        let compressed = zstd::bulk::compress(data, 1).unwrap();

        let ratio = compressed.len() as f32 / data.len() as f32;
        println!("{}", chunk.hash.to_hex());
        println!("{} / {} ({:.2}x)", data.len(), compressed.len(), ratio);

        Ok(compressed.len() as u64)
    }

    async fn store_file(&self, _ctx: &Self::Context, file: File<u64>) -> Result<(), Self::Error> {
        println!("----");
        println!("{}", file.hash.to_hex());

        let total_size = file.len;
        let compressed_size: u64 = file.chunks.iter().map(|c| c.1).sum();
        let ratio = compressed_size as f32 / total_size as f32;
        println!("{total_size} / {compressed_size} / {:.2}x", ratio);
        println!("====");

        Ok(())
    }
}
