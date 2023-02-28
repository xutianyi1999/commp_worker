use anyhow::{anyhow, Result};
use digest::Digest;
use sha2::Sha256;
use tokio::io::AsyncRead;

use crate::fr32_reader;

const NODE_SIZE: usize = 32;

struct Cache {
    cache: Vec<Option<[u8; 32]>>,
}

impl Cache {
    fn new() -> Self {
        Cache {
            cache: Vec::with_capacity(64)
        }
    }

    #[inline(always)]
    fn push(&mut self, mut cid: [u8; 32]) {
        let cache = &mut self.cache;

        for opt in cache.iter_mut() {
            match opt.take() {
                None => {
                    *opt = Some(cid);
                    return;
                }
                Some(left) => cid = piece_hash(&left, &cid)
            }
        }
        cache.push(Some(cid));
    }
}

#[inline(always)]
fn trim_to_fr32(buff: &mut [u8; 32]) {
    // strip last two bits, to ensure result is in Fr.
    buff[31] &= 0b0011_1111;
}

#[inline(always)]
fn hash(data: &[u8; 64]) -> [u8; 32] {
    let mut hashed = Sha256::digest(data);
    let hash: &mut [u8; 32] = hashed.as_mut_slice().try_into().unwrap();
    trim_to_fr32(hash);
    *hash
}

pub struct CommitmentReader<'a, R> {
    source: &'a mut R,
    current_tree: Cache,
}

#[inline(always)]
pub fn piece_hash(a: &[u8; NODE_SIZE], b: &[u8; NODE_SIZE]) -> [u8; 32] {
    let mut buf = [0u8; NODE_SIZE * 2];
    buf[..NODE_SIZE].copy_from_slice(a);
    buf[NODE_SIZE..].copy_from_slice(b);
    hash(&buf)
}

impl<'a, R: AsyncRead + Unpin> CommitmentReader<'a, R> {
    pub fn new(source: &'a mut R) -> Self {
        CommitmentReader {
            source,
            current_tree: Cache::new(),
        }
    }

    /// Attempt to generate the next hash, but only if the buffers are full.
    #[inline(always)]
    fn try_hash(&mut self, in_buff: &[u8; 64]) {
        let hash = hash(in_buff);
        self.current_tree.push(hash);
    }

    pub fn finish(self) -> Result<[u8; 32]> {
        let CommitmentReader { mut current_tree, .. } = self;

        let mut f = || {
            current_tree.cache.pop()?
        };
        f().ok_or_else(|| anyhow!("Get tree hash root failed"))
    }

    pub async fn consume(&mut self, blocks: u64) -> Result<()> {
        let mut buff = [0u8; 128];

        for _ in 0..blocks {
            fr32_reader::read_block(self.source, &mut buff).await?;
            self.try_hash((&buff[..64]).try_into().unwrap());
            self.try_hash((&buff[64..]).try_into().unwrap());
        }
        Ok(())
    }
}