use core::fmt;
use std::collections::HashMap;

use fjall::{TransactionalKeyspace, TransactionalPartitionHandle};
use tempfile::TempDir;

use super::*;

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Namespace(u64);
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

    segments: HashMap<segment::SegmentId, Segment>,
    last_segment: Option<segment::SegmentId>,

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

        Self {
            tempdir,
            database,
            chunks,
            files,
            segments: Default::default(),
            last_segment: None,
            namespaced_refcounts: Default::default(),
            segment_refcounts: Default::default(),
            chunk_refs: Default::default(),
            file_refs: Default::default(),
        }
    }
    pub fn with_namespace(&mut self, namespace: Namespace) -> NamespacedFileStore {
        NamespacedFileStore {
            filestore: self,
            config: Config::default(),
            namespace,
        }
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
    filestore: &'fs mut FileStore,
    config: Config,
    namespace: Namespace,
}

impl NamespacedFileStore<'_> {
    pub fn with_config(&mut self, config: Config) -> &mut Self {
        self.config = config;
        self
    }

    fn addref(&mut self, ty: refcounts::ReferenceCountType) {}

    pub fn upload_chunk(&mut self, contents: &[u8]) -> chunk::ChunkId {
        let hash_bytes = *blake3::hash(contents).as_bytes();
        let chunk_id = chunk::ChunkId {
            hash_algorithm: HashAlgorithm::Blake3,
            _padding: [0; 3],
            hash: hash_bytes,
        };

        let chunk_key = postcard::to_stdvec(&(self.namespace, chunk_id)).unwrap();

        let mut write_tx = self.filestore.database.write_tx().unwrap();
        if !write_tx
            .contains_key(&self.filestore.chunks, &chunk_key)
            .unwrap()
        {
            let segment_id =
                *self
                    .filestore
                    .last_segment
                    .get_or_insert_with(|| segment::SegmentId {
                        uuid: uuid::Uuid::new_v4().into_bytes(),
                    });

            // TODO:
            // self.addref(refcounts::ReferenceCountType::Segment(segment_id));
            let segment = self.filestore.segments.entry(segment_id).or_default();

            let offset_in_segment = segment.0.len() as u32;
            segment.0.extend_from_slice(contents);

            if segment.0.len() as u64 >= self.config.segment_size {
                self.filestore.last_segment.take();
            }

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

    pub fn upload_file(&mut self, contents: &[u8]) -> file::FileId {
        let hash_bytes = *blake3::hash(contents).as_bytes();
        let file_id = file::FileId {
            hash_algorithm: HashAlgorithm::Blake3,
            _padding: [0; 3],
            hash: hash_bytes,
        };

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

    pub fn read_chunk(&self, chunk_id: chunk::ChunkId) -> &[u8] {
        let chunk_key = postcard::to_stdvec(&(self.namespace, chunk_id)).unwrap();
        let read_tx = self.filestore.database.read_tx();

        let chunk = read_tx
            .get(&self.filestore.chunks, chunk_key)
            .unwrap()
            .unwrap();
        let chunk: chunk::Chunk = postcard::from_bytes(&chunk).unwrap();

        let segment = &self.filestore.segments[&chunk.segment_id];
        let start = chunk.offset_in_segment as usize;
        let range = start..start + chunk.size as usize;
        &segment.0[range]
    }

    pub fn read_file(&mut self, file_id: file::FileId) -> Vec<u8> {
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
                    contents.extend_from_slice(self.read_chunk(*chunk_id));
                }
                contents
            }
        }
    }

    pub fn assemble_file_from_chunks(&mut self, chunks: &[chunk::ChunkId]) -> file::FileId {
        let mut file_size = 0;
        let mut file_hash = blake3::Hasher::new();
        let chunks = chunks
            .into_iter()
            .copied()
            .map(|chunk_id| {
                let chunk_content = self.read_chunk(chunk_id);

                file_size += chunk_content.len() as u64;
                file_hash.update(chunk_content);

                file::FileChunk {
                    chunk_size: chunk_content.len() as u32,
                    chunk_id,
                }
            })
            .collect();

        let file_id = file::FileId {
            hash_algorithm: HashAlgorithm::Blake3,
            _padding: [0; 3],
            hash: *file_hash.finalize().as_bytes(),
        };

        let file = file::File {
            size: file_size,
            contents: file::FileContents::Chunked(chunks),
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filestore() {
        let mut global_fs = FileStore::new();

        let mut fs = global_fs.with_namespace(Namespace(0));
        let file_id = fs.upload_file(b"inlined file");
        assert_eq!(fs.read_file(file_id), b"inlined file");

        let mut fs = global_fs.with_namespace(Namespace(1));
        fs.with_config(Config {
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

    #[test]
    fn test_filestore_prechunked() {
        let mut global_fs = FileStore::new();

        let mut fs = global_fs.with_namespace(Namespace(0));
        let chunk_1 = fs.upload_chunk(b"some pre-chunked");
        let chunk_2 = fs.upload_chunk(b" content");

        let file_id = fs.assemble_file_from_chunks(&[chunk_1, chunk_2]);
        assert_eq!(fs.read_file(file_id), b"some pre-chunked content");
    }
}
