use bitarray::{MultiBitArray, SingleBitArray};
use bitfiddling::BitFiddling;
use std;

pub const BITS_PER_SLOT: usize = 9; //corresponds to false positive rate of 1/(2^9) == 1/512
const BLOCK_OFFSET_BITS: usize = (6); //6 seems fastest; corresponds to max offset 2^6-1 or 63
pub const SLOTS_PER_BLOCK: usize = (1 << BLOCK_OFFSET_BITS); //64 currently

#[allow(dead_code)] // for now
#[derive(Clone, Copy, Default, PartialEq, Debug)]
pub struct Block {
    /// The offset, in slots, from the first slot in this block to the end of whatever run that
    /// slot is part of.  0 could mean this is the end of that run or more likely that there is no
    /// such run.
    ///
    /// NB: The RSQF paper is a bit ambiguous about this so it bears clarification: when we talk
    /// about the "end of whatever run that slot [0] is part of" we don't mean a run specific to
    /// whatever quotient has its home slot in slot 0 of this block, we mean ANY run that happens
    /// to include slot 0 of this block in it.  Because of the way values are shifted to maintain
    /// invariants, it's entirely possible that slot 0 contains a remainder that's part of a run
    /// from a quotient whose home slot is in some previous block; we can't determine that just by
    /// looking at this offset.
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

    /// Given the index of a slot, computes not the actual offset of its run end, but the lower
    /// bound on that offset.  I don't understand why this is used, it comes from the
    /// `offset_lower_bound` function in the C implementation.  Perhaps this is more efficient than
    /// computing the actual end of the run?
    #[inline]
    pub fn get_slot_offset_lower_bound(&self, relative_index: usize) -> usize {
        debug_assert!(relative_index < SLOTS_PER_BLOCK);

        //NB: In cqf.c the function offset_lower_bound operates on a slot index, but in the body
        //everything it does is within the context of a single block.  Since in the Rust version
        //the Block structure's fields are private, it's more convenient to implement this here
        let block_offset = self.offset as usize; //NB: this isn't decoded; it could be MAX

        //Get the occupieds bit fields only up to this slot plus one more (don't know why +1)
        let occupieds = self.occupieds & bitmask!(relative_index + 1);

        if block_offset <= relative_index {
            //the run for the slot at the start of this block ends either before the slot at
            //`relative_index` or at it, but not past it.
            //count the runends after the runend at block_offset, up to and including
            //relative_index
            let runends = (self.runends & bitmask!(relative_index)) >> block_offset;

            //the lower bound offset is the number of occupied slots up to `relative_index` in this
            //block, minus the number of runends between the runend at block_offset and
            //`relative_index`.  I can't explain why this is the lower offset bound.
            occupieds.popcnt() - runends.popcnt()
        } else {
            //else the runend for the first slot in this block is after `relative_index` so the
            //lower bound is however many slots past `relative_index` is the runend for this
            //block's first slot, plus the number of occupieds between the two.
            //Again I can't explain this logic
            block_offset - relative_index + occupieds.popcnt()
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
