use bitarray::MultiBitArray;
use std::mem::size_of;

pub const BITS_PER_SLOT: usize = 9; //corresponds to false positive rate of 1/(2^9) == 1/512
const BLOCK_OFFSET_BITS: usize = (6); //6 seems fastest; corresponds to max offset 2^6-1 or 63
pub const SLOTS_PER_BLOCK: usize = (1 << BLOCK_OFFSET_BITS); //64 currently
const BLOCK_WORD_SIZE: usize = size_of::<u64>() * 8; //64 bits
const METADATA_WORDS_PER_BLOCK: usize = ((SLOTS_PER_BLOCK + BLOCK_WORD_SIZE - 1) / BLOCK_WORD_SIZE); //64 bits per word

#[allow(dead_code)] // for now
#[derive(Clone, Copy, Default, PartialEq, Debug)]
pub struct Block {
    offset: u8,
    occupieds: [u64; METADATA_WORDS_PER_BLOCK],
    runends: [u64; METADATA_WORDS_PER_BLOCK],
    slots: [u64; BITS_PER_SLOT],
}

impl Block {
    /// Gets the offset of this block; that is, the slot offset at which this block is located.
    /// Each block contains 64 slots, with the slot number stored only every 64th slot as an
    /// optimization.
    ///
    /// This block stores the 64 slots corresponding to the slots at `offset()` to `offset() + 63`.
    #[inline]
    #[allow(dead_code)]
    pub fn offset() -> u64 {
        //TODO: Not sure this can be implemented at the Block level; the C code computes this using
        //knowledge of adjacent blocks
        panic!("NYI");
    }

    /// Given the 0-based `relative_index` of a slot in this block, returns the value stored in
    /// that slot.  Note that the return type is `u64` but the actual range of possible values is
    /// determined by the parameter `rbits` which is typically on the order of 8 or 9
    #[inline]
    #[allow(dead_code)] // Just for now until higher level modules call this
    pub fn get_slot(&self, rbits: usize, relative_index: usize) -> u64 {
        self.slots.get_slot(rbits, relative_index)
    }

    /// Sets the value of the slot at `relative_index` to `val`.  Note the type of `val` is `u64`
    /// but its the `rbits` parameter that determines how many bits are actually stored in the slot
    ///
    /// # Returns
    ///
    /// The previous value stored in the slot `relative_index`
    #[inline]
    #[allow(dead_code)] // Just for now until higher level modules call this
    pub fn set_slot(&mut self, rbits: usize, relative_index: usize, val: u64) -> u64 {
        self.slots.set_slot(rbits, relative_index, val)
    }
}

#[cfg(test)]
mod block_tests {
    use super::*;
    extern crate rand;

    #[test]
    pub fn get_slot_empty_block_tests() {
        let block: Block = Block {
            ..Default::default()
        };

        for i in 0..SLOTS_PER_BLOCK {
            assert_eq!(0, block.get_slot(BITS_PER_SLOT, i));
        }
    }
    #[test]
    pub fn set_slot_empty_block_tests() {
        let mut block: Block = Block {
            ..Default::default()
        };

        block.set_slot(BITS_PER_SLOT, 0, 0x1ff);
        assert_eq!(0x1ff, block.get_slot(BITS_PER_SLOT, 0));
    }
}
