use std::cmp::min;
use std::io::{self};

use anyhow::{anyhow, ensure, Result};
use filecoin_hashers::{Hasher, HashFunction};
use filecoin_hashers::sha256::Sha256Hasher;
use fr32::Fr32Reader;
use tokio::io::AsyncRead;

use crate::bytes_amount::{PaddedBytesAmount, UnpaddedBytesAmount};

const NODE_SIZE: usize = 32;

pub type DefaultPieceHasher = Sha256Hasher;

struct Cache {
    cache: Vec<Option<<DefaultPieceHasher as Hasher>::Domain>>,
}

impl Cache {
    fn new() -> Self {
        Cache {
            cache: Vec::with_capacity(64)
        }
    }

    fn push(&mut self, mut cid: <DefaultPieceHasher as Hasher>::Domain) {
        let cache = &mut self.cache;

        for opt in cache.iter_mut() {
            match opt.take() {
                None => {
                    *opt = Some(cid);
                    return;
                }
                Some(left) => cid = piece_hash(&left.0, &cid.0)
            }
        }
        cache.push(Some(cid));
    }
}

/// Calculates comm-d of the data piped through to it.
/// Data must be bit padded and power of 2 bytes.
pub struct CommitmentReader<R> {
    source: Fr32Reader<R>,
    buffer: [u8; 64],
    buffer_pos: usize,
    current_tree: Cache,
}

pub fn piece_hash(a: &[u8; NODE_SIZE], b: &[u8; NODE_SIZE]) -> <DefaultPieceHasher as Hasher>::Domain {
    let mut buf = [0u8; NODE_SIZE * 2];
    buf[..NODE_SIZE].copy_from_slice(a);
    buf[NODE_SIZE..].copy_from_slice(b);
    <DefaultPieceHasher as Hasher>::Function::hash(&buf)
}

impl<R: AsyncRead + Unpin> CommitmentReader<R> {
    pub fn new(source: Fr32Reader<R>) -> Self {
        CommitmentReader {
            source,
            buffer: [0u8; 64],
            buffer_pos: 0,
            current_tree: Cache::new(),
        }
    }

    /// Attempt to generate the next hash, but only if the buffers are full.
    fn try_hash(&mut self) {
        if self.buffer_pos < 63 {
            return;
        }

        // WARNING: keep in sync with DefaultPieceHasher and its .node impl
        let hash = <DefaultPieceHasher as Hasher>::Function::hash(&self.buffer);
        self.current_tree.push(hash);
        self.buffer_pos = 0;
    }

    pub fn finish(self) -> Result<<DefaultPieceHasher as Hasher>::Domain> {
        ensure!(self.buffer_pos == 0, "not enough inputs provided");

        let CommitmentReader { mut current_tree, .. } = self;

        let mut f = || {
            current_tree.cache.pop()?
        };
        f().ok_or_else(|| anyhow!("Get tree hash root failed"))
    }

    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let start = self.buffer_pos;
        let left = 64 - self.buffer_pos;
        let end = start + min(left, buf.len());

        // fill the buffer as much as possible
        let r = self.source.read(&mut self.buffer[start..end]).await?;

        // write the data, we read
        buf[..r].copy_from_slice(&self.buffer[start..start + r]);

        self.buffer_pos += r;

        // try to hash
        self.try_hash();

        Ok(r)
    }

    pub async fn consume(&mut self) -> io::Result<()> {
        let mut buff = [0u8; 128];

        loop {
            let len = self.read(&mut buff).await?;

            if len == 0 {
                return Ok(());
            }
        }
    }
}

#[allow(unused)]
fn unpadded_piece_size(size: u64) -> UnpaddedBytesAmount {
    if size <= 127 {
        return UnpaddedBytesAmount(127)
    }

    let mut padded_piece_size = (size + 126) / 127 * 128;

    if padded_piece_size.count_ones() != 1 {
        padded_piece_size = 1 << 64-padded_piece_size.leading_zeros();
    }
    UnpaddedBytesAmount::from(PaddedBytesAmount(padded_piece_size))
}