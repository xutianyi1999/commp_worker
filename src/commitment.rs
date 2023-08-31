use anyhow::{anyhow, ensure, Result};
use arrayvec::ArrayVec;
use digest::Digest;
use sha2::Sha256;

use crate::fr32_util;

#[inline]
fn trim_to_fr32(buff: &mut [u8; 32]) {
    // strip last two bits, to ensure result is in Fr.
    buff[31] &= 0b0011_1111;
}

#[inline]
pub fn hash(data: &[u8; 64]) -> [u8; 32] {
    let mut hashed = Sha256::digest(data);
    let hash: &mut [u8; 32] = hashed.as_mut_slice().try_into().unwrap();
    trim_to_fr32(hash);
    *hash
}

#[inline]
fn piece_hash(
    a: &[u8; 32],
    b: &[u8; 32],
    compute_buff: &mut [u8; 64],
) -> [u8; 32] {
    compute_buff[..32].copy_from_slice(a);
    compute_buff[32..].copy_from_slice(b);
    hash(compute_buff)
}

struct Cache {
    cache: ArrayVec<Option<[u8; 32]>, 64>,
}

impl Cache {
    fn new() -> Self {
        Cache {
            cache: ArrayVec::new(),
        }
    }

    #[inline]
    fn push(&mut self, mut cid: [u8; 32], compute_buff: &mut [u8; 64]) {
        let cache = &mut self.cache;

        for opt in cache.iter_mut() {
            match opt.take() {
                None => {
                    *opt = Some(cid);
                    return;
                }
                Some(left) => cid = piece_hash(&left, &cid, compute_buff)
            }
        }

        unsafe {
            cache.push_unchecked(Some(cid));
        }
    }
}

pub struct Commitment {
    current_tree: Cache,
}

impl Commitment {
    pub fn new() -> Self {
        Commitment {
            current_tree: Cache::new(),
        }
    }

    /// Attempt to generate the next hash, but only if the buffers are full.
    #[inline]
    fn put_leaf(&mut self, in_buff: &mut [u8; 64]) {
        let hash = hash(in_buff);
        self.current_tree.push(hash, in_buff);
    }

    pub fn finish(self, mut count: u64) -> Result<[u8; 32]> {
        if count == 0 {
            let Commitment { mut current_tree, .. } = self;

            let mut f = || {
                current_tree.cache.pop()?
            };

            return f().ok_or_else(|| anyhow!("Get tree hash root failed"));
        }

        const ZERO_CACHE: [[u8; 32]; 64] = gen_merkletree_cache::generate!(64);
        let mut compute_buff = [0u8; 64];
        let mut next: Option<([u8; 32], usize)> = None;

        macro_rules! sub {
            ($value: expr) => {
                ensure!(count >= $value);
                count -= $value;
            };
        }

        for (layer, opt) in self.current_tree.cache.into_iter().enumerate() {
            let hash = match opt {
                None => {
                    match next {
                        None => continue,
                        Some((left, _)) => {
                            sub!(2u64.pow(layer as u32));
                            piece_hash(&left, &ZERO_CACHE[layer], &mut compute_buff)
                        }
                    }
                }
                Some(left) => {
                    match next {
                        None => {
                            sub!(2u64.pow(layer as u32));
                            piece_hash(&left, &ZERO_CACHE[layer], &mut compute_buff)
                        }
                        Some((right, _)) => piece_hash(&left, &right, &mut compute_buff)
                    }
                }
            };
            next = Some((hash, layer + 1));
        }

        if next.is_none() {
            return Ok(ZERO_CACHE[count.ilog2() as usize]);
        }

        let (mut next, mut layer) = next.unwrap();

        while count > 0 {
            sub!(2u64.pow(layer as u32));
            next = piece_hash(&next, &ZERO_CACHE[layer], &mut compute_buff);
            layer += 1;
        }

        Ok(next)
    }

    #[inline]
    pub fn consume(&mut self, in_buff: &[u8; 128], out_buff: &mut [u8; 128]) {
        fr32_util::process_block(in_buff, out_buff);
        self.put_leaf((&mut out_buff[..64]).try_into().unwrap());
        self.put_leaf((&mut out_buff[64..]).try_into().unwrap());
    }
}