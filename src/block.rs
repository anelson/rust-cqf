use bitarray::{MultiBitArray, SingleBitArray};
use bitfiddling::BitFiddling;
use std;

pub const BITS_PER_SLOT: usize = 9; //corresponds to false positive rate of 1/(2^9) == 1/512
const BLOCK_OFFSET_BITS: usize = (6); //6 seems fastest; corresponds to max offset 2^6-1 or 63
pub const SLOTS_PER_BLOCK: usize = (1 << BLOCK_OFFSET_BITS); //64 currently

#[allow(dead_code)] // for now
#[derive(Clone, Copy, Default, PartialEq, Debug)]
pub struct Block {
    offset: u8,
    occupieds: u64,
    runends: u64,
    slots: [u64; BITS_PER_SLOT],
}

#[allow(dead_code)] // for now
impl Block {
    /// Returns the distance between `i` the first slot in this block, and the slot containing the
    /// run end for the run in which `i` is included, or `0` if `i` is not occupied.
    ///
    /// Note that this is stored per-block for only the first slot in each block.  For the other
    /// slots the offset can be deduced via the procedure explained in Figure 2 of the RSQF paper.
    ///
    /// # Remarks
    ///
    /// To save space, this is stored at `u8`.  It's technically possible for some particularly
    /// long runs to result in a run end that is more than 255 slots away from `i`.  In that case,
    /// this method returns `None`, and the implmenetation should then jump to the next block and
    /// try its offset, iteratively until one with a non-MAX offset is found.
    #[inline]
    #[allow(dead_code)]
    pub fn offset(&self) -> Option<usize> {
        if self.offset < std::u8::MAX {
            Some(self.offset as usize)
        } else {
            None
        }
    }

    #[inline]
    pub fn set_offset(&mut self, offset: usize) -> () {
        if offset < std::u8::MAX as usize {
            self.offset = offset as u8;
        } else {
            self.offset = std::u8::MAX
        }
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

    /// Given the relative index of a slot, checks the `occupies` bitmap and returns the number of
    /// `1` bits up to `relative_index`
    #[inline]
    pub fn get_occupied_rank(&self, relative_index: usize) -> usize {
        self.occupieds.popcnt_first_n(relative_index)
    }

    /// Tests if the runend bit for the specified relative index is set.
    pub fn is_runend(&self, relative_index: usize) -> bool {
        debug_assert!(relative_index < SLOTS_PER_BLOCK);

        self.runends.get_bit(relative_index)
    }

    /// Sets the runend bit for the specified relative index.
    pub fn set_runend(&mut self, relative_index: usize, val: bool) -> () {
        debug_assert!(relative_index < SLOTS_PER_BLOCK);

        self.runends.set_bit(relative_index, val);
    }

    /// Tests if the occupied bit for the specified relative index is set.
    pub fn is_occupied(&self, relative_index: usize) -> bool {
        debug_assert!(relative_index < SLOTS_PER_BLOCK);

        self.occupieds.get_bit(relative_index)
    }

    /// Sets the occupied bit for the specified relative index.
    pub fn set_occupied(&mut self, relative_index: usize, val: bool) -> () {
        debug_assert!(relative_index < SLOTS_PER_BLOCK);

        self.occupieds.set_bit(relative_index, val);
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

    #[test]
    pub fn get_set_bit_fields() {
        let mut block: Block = Block {
            ..Default::default()
        };

        for i in 0..SLOTS_PER_BLOCK {
            assert_eq!(false, block.is_runend(i));
            assert_eq!(false, block.is_occupied(i));

            block.set_occupied(i, true);

            assert_eq!(false, block.is_runend(i));
            assert_eq!(true, block.is_occupied(i));

            block.set_runend(i, true);

            assert_eq!(true, block.is_runend(i));
            assert_eq!(true, block.is_occupied(i));

            block.set_occupied(i, false);
            block.set_runend(i, false);

            assert_eq!(false, block.is_runend(i));
            assert_eq!(false, block.is_occupied(i));
        }
    }
}
