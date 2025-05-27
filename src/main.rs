use std::sync::{Arc, RwLock};

use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::handler::Handler;
use axum::http::{Method, Response, StatusCode, Uri};
use axum::response::IntoResponse;
// use kycok::new_datamodel::mem_impl::{FileStore, Namespace};
use kycok::new_datamodel::fjall_impl::{FileStore, Namespace};

// type FileStoreState = Arc<RwLock<FileStore>>;
type FileStoreState = Arc<FileStore>;

#[tokio::main]
async fn main() {
    // let app = Router::new().route("/{bucket}/{*path}", get(download_file).put(upload_file));

    let filestore = FileStoreState::default();
    let app = app.with_state(filestore).into_make_service();

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn app(
    State(filestore): State<FileStoreState>,
    method: Method,
    uri: Uri,
    body: Body,
) -> Response<Body> {
    let path = uri.path();
    let mut splits = path.splitn(3, "/");
    let _ = splits.next();

    match method {
        Method::GET => {
            if let Some(query) = uri.query() {
                if query.starts_with("location") {
                    return r#"<LocationConstraint>whatever</LocationConstraint>"#.into_response();
                }
                if query.starts_with("object-lock") {
                    return r#"<ObjectLockConfiguration />"#.into_response();
                }
                if query.starts_with("versioning") {
                    return r#"<VersioningConfiguration />"#.into_response();
                }
            } else {
                let namespace = splits.next().unwrap().parse().unwrap();
                let filestore = FileStore::with_namespace(&filestore, Namespace(namespace));
                let path = splits.next().unwrap();

                let file = filestore.read_named_file(path);

                return ([("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")], file)
                    .into_response();
            }
        }
        Method::HEAD => return ().into_response(),
        Method::PUT => {
            let bytes = to_bytes(body, usize::MAX).await.unwrap();

            let namespace = splits.next().unwrap().parse().unwrap();
            let filestore = FileStore::with_namespace(&filestore, Namespace(namespace));
            let path = splits.next().unwrap();

            let file_id = filestore.upload_file(&bytes);
            filestore.associate_filename(file_id, path);

            return ().into_response();
        }
        _ => {}
    }

    dbg!((&method, &uri));
    StatusCode::BAD_REQUEST.into_response()
}

// async fn upload_file(
//     Path(bucket): Path<u64>,
//     Path(path): Path<String>,
//     body: Body,
// ) -> &'static str {
//     "OK"
// }

// async fn download_file(Path(bucket): Path<u64>, Path(path): Path<String>, body: Body) -> Body {
//     todo!()
// }

// async fn upload_none(body: BodyStream) -> &'static str {
//     let chunker = Chunker::new(PrintBackend);
//     let config = FileConfig {
//         chunking_strategy: ChunkingStrategy::None,
//     };
//     chunker
//         .save_file(&config, &(), make_read(body))
//         .await
//         .unwrap();

//     "OK"
// }

// async fn upload_fixed(Path(chunk_size): Path<u32>, body: BodyStream) -> &'static str {
//     let chunker = Chunker::new(PrintBackend);
//     let config = FileConfig {
//         chunking_strategy: ChunkingStrategy::Fixed(chunk_size * 1024),
//     };
//     chunker
//         .save_file(&config, &(), make_read(body))
//         .await
//         .unwrap();

//     "OK"
// }

// async fn upload_cdc(body: BodyStream) -> &'static str {
//     let chunker = Chunker::new(PrintBackend);
//     let config = FileConfig {
//         chunking_strategy: Default::default(),
//     };
//     chunker
//         .save_file(&config, &(), make_read(body))
//         .await
//         .unwrap();

//     "OK"
// }

// fn make_read(body: BodyStream) -> impl AsyncRead {
//     let stream = body
//         .map(|result| result.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err)));
//     StreamReader::new(stream)
// }

// struct PrintBackend;

// #[async_trait::async_trait]
// impl Backend for PrintBackend {
//     type Context = ();
//     type StoredChunk = u64;
//     type FileId = ();
//     type Error = std::io::Error;

//     async fn store_chunk(&self, _ctx: &Self::Context, chunk: Chunk) -> Result<u64, Self::Error> {
//         let data = &chunk.data;
//         let compressed = zstd::bulk::compress(data, 1).unwrap();

//         let ratio = compressed.len() as f32 / data.len() as f32;
//         println!("{}", chunk.hash.to_hex());
//         println!("{} / {} ({:.2}x)", data.len(), compressed.len(), ratio);

//         Ok(compressed.len() as u64)
//     }

//     async fn store_file(&self, _ctx: &Self::Context, file: File<u64>) -> Result<(), Self::Error> {
//         println!("----");
//         println!("{}", file.hash.to_hex());

//         let total_size = file.len;
//         let compressed_size: u64 = file.chunks.iter().map(|c| c.1).sum();
//         let ratio = compressed_size as f32 / total_size as f32;
//         println!("{total_size} / {compressed_size} / {:.2}x", ratio);
//         println!("====");

//         Ok(())
//     }
// }
