use std::net::SocketAddr;
use std::pin::pin;

use axum::extract::BodyStream;
use axum::routing::post;
use axum::Router;
use futures_util::StreamExt;
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
    let mut chunks = kycok::cdc::make_stream(read);
    let mut chunks = pin!(chunks.as_stream());

    let mut total_size = 0;
    let mut compressed_size = 0;

    let mut hasher = blake3::Hasher::new();

    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.unwrap();

        let data = &chunk.data;
        hasher.update(data);

        let chunk_hash = blake3::hash(data);
        let compressed = zstd::bulk::compress(data, 1).unwrap();

        total_size += data.len();
        compressed_size += compressed.len();
        let ratio = compressed.len() as f32 / data.len() as f32;

        println!("{}", chunk_hash.to_hex());
        println!(
            "{} .. {} ({} / {:.2}x)",
            chunk.offset,
            data.len(),
            compressed.len(),
            ratio,
        );
    }

    println!("----");
    println!("{}", hasher.finalize().to_hex());
    let ratio = compressed_size as f32 / total_size as f32;
    println!("{total_size} / {compressed_size} / {:.2}x", ratio);
    println!("====");

    "OK"
}
