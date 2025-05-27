use serde::{Deserialize, Serialize};
use sha1::{Digest as _, Sha1};

pub mod fjall_impl;
pub mod mem_impl;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum HashAlgorithm {
    Sha1 = 0,
    Blake3 = 1,
}

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct ContentHash {
    pub hash_algorithm: HashAlgorithm,
    pub _padding: [u8; 3],
    pub hash_bytes: [u8; 28],
}

impl ContentHash {
    pub fn new(contents: &[u8]) -> Self {
        let hash_algorithm = HashAlgorithm::Blake3;

        let mut hash_bytes = [0; 28];
        match hash_algorithm {
            HashAlgorithm::Sha1 => {
                let sha1_hash = Sha1::digest(contents);
                (&mut hash_bytes[..20]).copy_from_slice(sha1_hash.as_slice());
            }
            HashAlgorithm::Blake3 => {
                let blake3_hash = blake3::hash(contents);
                hash_bytes.copy_from_slice(&blake3_hash.as_bytes()[..28]);
            }
        }
        Self {
            hash_algorithm: hash_algorithm,
            _padding: [0; 3],
            hash_bytes,
        }
    }
}

pub mod chunk {
    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    #[repr(u8)]
    pub enum Compression {
        None = 0,
        Zstd = 1,
    }

    /// The content-addressable ID of a `Chunk`
    #[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
    #[repr(C)]
    pub struct ChunkId(pub ContentHash);
    impl ChunkId {
        pub fn from_contents(contents: &[u8]) -> Self {
            Self(ContentHash::new(contents))
        }
    }

    /// Chunk metadata, in particular where it is stored
    #[derive(Debug, Serialize, Deserialize)]
    pub struct Chunk {
        pub size: u32,
        pub compression: Compression,
        pub compressed_size: u32,
        pub segment_id: segment::SegmentId,
        pub offset_in_segment: u32,
    }
}

pub mod file {
    use super::*;

    /// The content-addressable ID of a `File`
    #[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
    #[repr(C)]
    pub struct FileId(pub ContentHash);
    impl FileId {
        pub fn from_contents(contents: &[u8]) -> Self {
            Self(ContentHash::new(contents))
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct File {
        pub size: u64,
        pub contents: FileContents,
    }

    #[derive(Serialize, Deserialize)]
    pub enum FileContents {
        Inline(Vec<u8>),
        Chunked(Vec<FileChunk>),
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct FileChunk {
        pub chunk_size: u32,
        pub chunk_id: chunk::ChunkId,
    }
}

pub mod segment {
    use super::*;

    #[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
    #[repr(C)]
    pub struct SegmentId {
        pub uuid: [u8; 16],
    }
}

pub mod refcounts {
    use super::*;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
    pub enum ReferenceCountType {
        Chunk(chunk::ChunkId),
        File(file::FileId),
        Segment(segment::SegmentId),
    }
}

pub mod gc {
    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Timestamp(u32);

    #[derive(Debug, Serialize, Deserialize)]
    pub struct ChunkRef {
        pub chunk_id: chunk::ChunkId,
        pub expires: Timestamp,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct FileReference {
        pub path: String,
        pub file_id: file::FileId,
        pub expires: Timestamp,
    }
}

mod dbg {
    use super::*;
    use core::fmt;

    impl fmt::Debug for segment::SegmentId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "SegmentId({:x})", base16ct::HexDisplay(&self.uuid))
        }
    }

    impl fmt::Debug for ContentHash {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self.hash_algorithm {
                HashAlgorithm::Sha1 => {
                    write!(f, "SHA1:{:x}", base16ct::HexDisplay(&self.hash_bytes[..20]))
                }
                HashAlgorithm::Blake3 => {
                    write!(f, "BLAKE3:{:x}", base16ct::HexDisplay(&self.hash_bytes))
                }
            }
        }
    }

    impl fmt::Debug for chunk::ChunkId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "ChunkId({:?})", self.0)
        }
    }
    impl fmt::Debug for file::FileId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "FileId({:?})", self.0)
        }
    }

    impl fmt::Debug for file::FileContents {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Inline(inline) => {
                    let len = inline.len().min(64);
                    let printed = String::from_utf8_lossy(&inline[..len]);

                    write!(
                        f,
                        "Inline({printed:?}{})",
                        if len < inline.len() { "…" } else { "" }
                    )
                }
                Self::Chunked(chunks) => f.debug_tuple("Chunked").field(chunks).finish(),
            }
        }
    }
}
