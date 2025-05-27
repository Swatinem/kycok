use core::fmt;
use std::collections::HashMap;
use std::sync::Mutex;

use fjall::{TransactionalKeyspace, TransactionalPartitionHandle};
use tempfile::TempDir;

use super::*;

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Namespace(pub u64);
impl fmt::Debug for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Namespace({})", self.0)
    }
}

#[derive(Default)]
pub struct Segment(Vec<u8>);
impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let len = self.0.len().min(64);
        let printed = String::from_utf8_lossy(&self.0[..len]);

        write!(
            f,
            "Segment({printed:?}{})",
            if len < self.0.len() { "…" } else { "" }
        )
    }
}

pub struct FileStore {
    tempdir: TempDir,
    database: TransactionalKeyspace,
    chunks: TransactionalPartitionHandle,
    files: TransactionalPartitionHandle,
    named_files: TransactionalPartitionHandle,

    segments: Mutex<HashMap<segment::SegmentId, Segment>>,
    last_segment: Mutex<Option<segment::SegmentId>>,

    namespaced_refcounts: HashMap<(Namespace, refcounts::ReferenceCountType), u32>,
    segment_refcounts: HashMap<segment::SegmentId, u32>,

    chunk_refs: HashMap<(Namespace, chunk::ChunkId), gc::ChunkRef>,
    file_refs: HashMap<(Namespace, String), gc::FileReference>,
}

impl FileStore {
    pub fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let database = fjall::Config::new(&tempdir).open_transactional().unwrap();
        let chunks = database
            .open_partition("chunks", Default::default())
            .unwrap();
        let files = database
            .open_partition("files", Default::default())
            .unwrap();
        let named_files = database
            .open_partition("named_files", Default::default())
            .unwrap();

        Self {
            tempdir,
            database,
            chunks,
            files,
            named_files,

            segments: Default::default(),
            last_segment: Default::default(),
            namespaced_refcounts: Default::default(),
            segment_refcounts: Default::default(),
            chunk_refs: Default::default(),
            file_refs: Default::default(),
        }
    }

    pub fn with_namespace(slf: &FileStore, namespace: Namespace) -> NamespacedFileStore {
        NamespacedFileStore {
            filestore: slf,
            config: Config::default(),
            namespace,
        }
    }
}
impl Default for FileStore {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Config {
    inline_size: u64,
    chunk_size: u64,
    segment_size: u64,
}

impl Default for Config {
    fn default() -> Self {
        const MEG: u64 = 2 ^ 20;
        const GIG: u64 = 2 ^ 30;
        Self {
            inline_size: 256,
            chunk_size: 8 * MEG,
            segment_size: GIG,
        }
    }
}

pub struct NamespacedFileStore<'fs> {
    filestore: &'fs FileStore,
    config: Config,
    namespace: Namespace,
}

impl NamespacedFileStore<'_> {
    pub fn with_config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    // fn addref(&mut self, ty: refcounts::ReferenceCountType) {}

    pub fn upload_chunk(&self, contents: &[u8]) -> chunk::ChunkId {
        let chunk_id = chunk::ChunkId::from_contents(contents);

        let chunk_key = postcard::to_stdvec(&(self.namespace, chunk_id)).unwrap();

        let mut write_tx = self.filestore.database.write_tx().unwrap();
        if !write_tx
            .contains_key(&self.filestore.chunks, &chunk_key)
            .unwrap()
        {
            let (segment_id, offset_in_segment) = {
                let mut last_segment = self.filestore.last_segment.lock().unwrap();
                let mut segments = self.filestore.segments.lock().unwrap();

                let segment_id = *last_segment.get_or_insert_with(|| segment::SegmentId {
                    uuid: uuid::Uuid::new_v4().into_bytes(),
                });

                // TODO:
                // self.addref(refcounts::ReferenceCountType::Segment(segment_id));
                let segment = segments.entry(segment_id).or_default();

                let offset_in_segment = segment.0.len() as u32;
                segment.0.extend_from_slice(contents);

                if segment.0.len() as u64 >= self.config.segment_size {
                    last_segment.take();
                }
                (segment_id, offset_in_segment)
            };

            let chunk = chunk::Chunk {
                size: contents.len() as u32,
                compression: chunk::Compression::None,
                compressed_size: contents.len() as u32,
                segment_id,
                offset_in_segment,
            };
            let chunk = postcard::to_stdvec(&chunk).unwrap();

            write_tx.insert(&self.filestore.chunks, chunk_key, chunk);
        }
        // TODO:
        // self.addref(refcounts::ReferenceCountType::Chunk(chunk_id));
        write_tx.commit().unwrap().unwrap();

        chunk_id
    }

    pub fn upload_file(&self, contents: &[u8]) -> file::FileId {
        let file_id = file::FileId::from_contents(contents);

        let file_size = contents.len() as u64;
        let contents = if file_size <= self.config.inline_size {
            file::FileContents::Inline(contents.into())
        } else {
            let chunks = contents
                .chunks(self.config.chunk_size as usize)
                .map(|chunk| file::FileChunk {
                    chunk_size: chunk.len() as u32,
                    chunk_id: self.upload_chunk(chunk),
                })
                .collect();
            file::FileContents::Chunked(chunks)
        };

        let file = file::File {
            size: file_size,
            contents,
        };
        let file = postcard::to_stdvec(&file).unwrap();

        let file_key = postcard::to_stdvec(&(self.namespace, file_id)).unwrap();
        let mut write_tx = self.filestore.database.write_tx().unwrap();
        write_tx.insert(&self.filestore.files, file_key, file);

        // TODO:
        // self.addref(refcounts::ReferenceCountType::File(file_id));
        write_tx.commit().unwrap().unwrap();

        file_id
    }

    pub fn read_chunk(&self, chunk_id: chunk::ChunkId) -> Vec<u8> {
        let chunk_key = postcard::to_stdvec(&(self.namespace, chunk_id)).unwrap();
        let read_tx = self.filestore.database.read_tx();

        let chunk = read_tx
            .get(&self.filestore.chunks, chunk_key)
            .unwrap()
            .unwrap();
        let chunk: chunk::Chunk = postcard::from_bytes(&chunk).unwrap();

        let segments = self.filestore.segments.lock().unwrap();
        let segment = &segments[&chunk.segment_id];
        let start = chunk.offset_in_segment as usize;
        let range = start..start + chunk.size as usize;
        (&segment.0[range]).into()
    }

    pub fn read_file(&self, file_id: file::FileId) -> Vec<u8> {
        let file_key = postcard::to_stdvec(&(self.namespace, file_id)).unwrap();
        let read_tx = self.filestore.database.read_tx();

        let file = read_tx
            .get(&self.filestore.files, file_key)
            .unwrap()
            .unwrap();
        let file: file::File = postcard::from_bytes(&file).unwrap();

        match &file.contents {
            file::FileContents::Inline(contents) => contents.clone(),
            file::FileContents::Chunked(chunks) => {
                let mut contents = Vec::with_capacity(file.size as usize);
                for file::FileChunk { chunk_id, .. } in chunks {
                    contents.extend_from_slice(&self.read_chunk(*chunk_id));
                }
                contents
            }
        }
    }

    // pub fn assemble_file_from_chunks(&self, chunks: &[chunk::ChunkId]) -> file::FileId {
    //     let mut file_size = 0;
    //     let mut file_hash = blake3::Hasher::new();
    //     let chunks = chunks
    //         .into_iter()
    //         .copied()
    //         .map(|chunk_id| {
    //             let chunk_content = self.read_chunk(chunk_id);

    //             file_size += chunk_content.len() as u64;
    //             file_hash.update(&chunk_content);

    //             file::FileChunk {
    //                 chunk_size: chunk_content.len() as u32,
    //                 chunk_id,
    //             }
    //         })
    //         .collect();

    //     let file_id = file::FileId {
    //         hash_algorithm: HashAlgorithm::Blake3,
    //         _padding: [0; 3],
    //         hash: *file_hash.finalize().as_bytes(),
    //     };

    //     let file = file::File {
    //         size: file_size,
    //         contents: file::FileContents::Chunked(chunks),
    //     };
    //     let file = postcard::to_stdvec(&file).unwrap();

    //     let file_key = postcard::to_stdvec(&(self.namespace, file_id)).unwrap();
    //     let mut write_tx = self.filestore.database.write_tx().unwrap();
    //     write_tx.insert(&self.filestore.files, file_key, file);

    //     // TODO:
    //     // self.addref(refcounts::ReferenceCountType::File(file_id));
    //     write_tx.commit().unwrap().unwrap();

    //     file_id
    // }

    pub fn associate_filename(&self, file_id: file::FileId, name: &str) {
        let key = postcard::to_stdvec(&(self.namespace, name)).unwrap();
        let value = postcard::to_stdvec(&file_id).unwrap();

        self.filestore.named_files.insert(key, value).unwrap();
    }

    pub fn read_named_file(&self, name: &str) -> Vec<u8> {
        let key = postcard::to_stdvec(&(self.namespace, name)).unwrap();

        let file_id = self.filestore.named_files.get(key).unwrap().unwrap();
        let file_id: file::FileId = postcard::from_bytes(&file_id).unwrap();

        self.read_file(file_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filestore() {
        let global_fs = FileStore::new();

        let fs = FileStore::with_namespace(&global_fs, Namespace(0));
        let file_id = fs.upload_file(b"inlined file");
        assert_eq!(fs.read_file(file_id), b"inlined file");

        let fs = FileStore::with_namespace(&global_fs, Namespace(1)).with_config(Config {
            inline_size: 4,
            chunk_size: 16,
            segment_size: 32,
        });
        let contents = b"chunked, and deduped file contents...";

        let file_id = fs.upload_file(contents);
        assert_eq!(fs.read_file(file_id), contents);

        let file_id = fs.upload_file(contents);
        assert_eq!(fs.read_file(file_id), contents);
    }

    // #[test]
    // fn test_filestore_prechunked() {
    //     let mut global_fs = FileStore::new();

    //     let mut fs = FileStore::with_namespace(&global_fs, Namespace(0));
    //     let chunk_1 = fs.upload_chunk(b"some pre-chunked");
    //     let chunk_2 = fs.upload_chunk(b" content");

    //     let file_id = fs.assemble_file_from_chunks(&[chunk_1, chunk_2]);
    //     assert_eq!(fs.read_file(file_id), b"some pre-chunked content");
    // }
}
