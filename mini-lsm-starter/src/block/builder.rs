#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // |             Data Section             |              Offset Section             |      Extra      |
    // ----------------------------------------------------------------------------------------------------
    // | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
    // ----------------------------------------------------------------------------------------------------
    //
    // -----------------------------------------------------------------------
    // |                           Entry #1                            | ... |
    // -----------------------------------------------------------------------
    // | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
    // -----------------------------------------------------------------------

    fn estimated_size(&self) -> usize {
        self.data.len() + // key + value
        SIZEOF_U16 * self.offsets.len() +  // offset section
        SIZEOF_U16 // extra
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must be not empty");
        // 3 * SIZEOF_U16 = key_len (2B) + value_len (2B)+ Offset
        // Unless the first key-value pair exceeds the target block size,
        // you should ensure that the encoded block size is always less than or equal to target_size
        if self.estimated_size() + key.len() + value.len() + 3 * SIZEOF_U16 > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        // offset
        self.offsets.push(self.data.len() as u16);
        // key_len
        self.data.put_u16(key.len() as u16);
        // key
        self.data.put(key.raw_ref());
        // value_len
        self.data.put_u16(value.len() as u16);
        // value
        self.data.put(value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("Block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
