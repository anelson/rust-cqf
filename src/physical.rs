//! `PhysicalData` contains the actual in-memory data structures which represent the RSQF.  It
//! tries to abstract away most of the bit-fiddling trickery and expose a higher-level interface
//! that might be understandable by someone who's read the RSQF paper but doesn't squint at
//! low-level C code for a living
use block;
use std::vec::Vec;

#[allow(dead_code)] // for now
#[derive(Default, PartialEq)]
pub struct PhysicalData {
    blocks: Vec<block::Block>,
    rbits: usize,
}

impl PhysicalData {
    pub fn new(nblocks: usize, rbits: usize) -> PhysicalData {
        // Allocate a vector of Block structures.  Because of how this structure will be used,
        // the contents should be populated in advance
        let blocks = vec![Default::default(); nblocks];
        PhysicalData { blocks, rbits }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_blocks() {
        let pd = PhysicalData::new(10, block::BITS_PER_SLOT);

        assert_eq!(pd.blocks.len(), 10);
        assert_eq!(pd.blocks[0], Default::default());
    }
}
