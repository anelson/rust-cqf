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

/// Conveys the result of a scan through a block for a runend of a certain rank.  Scans either conclude in a match,
/// or not enough runends are found so the scan returns how many were found
#[derive(Debug, PartialEq)]
pub enum ScanForRunendResult {
    /// Indicates the runend of the desired rank has been found, and returns its index relative to
    /// the start of the block in which it was found.
    FoundIt(usize),

    /// Indicates the runend of the desired rank was not found, and returns the number of set
    /// runend bits the scan passed while searching.  The caller can use this to continue the scan
    /// in the next block.
    KeepScanning(usize),
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

    /// Given the index of a slot, attempts to compute the length of the run starting at `index`.
    /// I a write "attempts" because this avoids any complex computations and computes a lower
    /// bound on the length of the run.  The run might be longer than this method computes, but it
    /// will never be shorter.
    ///
    /// # Returns
    ///
    /// The remaining length of the run of which `relative_index` is one element.  This length
    /// includes `relative_index`, so if there is a run with a single element at `relative_index`,
    /// this method returns `1`.  If `relative_index` is unused, returns `0`.
    ///
    /// # Remarks
    ///
    /// This corresponds to the `cqf.c` function `offset_lower_bound`.
    #[inline]
    pub fn get_slot_runlength_lower_bound(&self, relative_index: usize) -> usize {
        debug_assert!(relative_index < SLOTS_PER_BLOCK);

        //NB: In cqf.c the function offset_lower_bound operates on a slot index, but in the body
        //everything it does is within the context of a single block.  Since in the Rust version
        //the Block structure's fields are private, it's more convenient to implement this here
        let block_offset = self.offset as usize; //NB: this isn't decoded; it could be MAX

        //Get the occupieds bit fields only up to this slot.  The `+ 1` here is because bitmask!
        //takes as a parameter the number of bits to set, so `relative_index = 0` would mean we get
        //a bitmask of all zeros, but that's not what we want, we want to mask out all occupied
        //bits up to and including `relative_index`
        let occupieds = self.occupieds & bitmask!(relative_index + 1);

        if block_offset <= relative_index {
            //the run for the slot at the start of this block ends either before the slot at
            //`relative_index` or at it, but not past it.
            //count the runends after the runend at block_offset, up to but not including
            let runends = (self.runends & bitmask!(relative_index)) >> block_offset;

            //the lower bound offset is the number of occupied slots up to `relative_index` in this
            //block, minus the number of runends between the runend at block_offset and
            //`relative_index`.  The idea here is that we count `occupieds` bits up to and including
            //this index, but `runends` bits up to but NOT including this index.  So if this index
            //is occupied, and it is its own runend (meaning run length of 1), this method returns
            //1.
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

    /// Shifts some or all of the slots right by one slot.  This obviously will shift one slot off
    /// the end of this block.
    ///
    /// # Arguments
    ///
    /// `rbits` - The size of each slot in bits
    /// `start_relative_index` - The index of the slot in this block where the shift operation
    /// should start.  `0` starts the shift from the start of the block.
    /// `slot_count` - The number of slots to shift.  Note that this can be more than the number of
    /// slots in a block; if so, `shift_slots` will shift from `start_relative_index` to the end of
    /// the block, and include in its return value the `slot_count` minus the number of slots it
    /// shifted already; this lets the caller call this method in a loop iteratively shifting all
    /// of the slots.
    /// `previous_shifted_slot` - If a previous block also shifted its slots, this is the value of
    /// the slot shifted off the end of that block, which will be placed into this block at
    /// `start_relative_index` after its contents are shifted up one.  Set to `0` for the block
    /// where the shifting operation starts.
    ///
    /// # Returns
    ///
    /// Returns a tuple which makes it easy to perform the shift iteratively.
    ///
    /// `(remaining_slot_count, shifted_slot)`:
    ///
    /// * `remaining_slot_count` - How many slots are still to be shifted, after this block's shift
    /// operation has completed.  A convenience value; it's `slot_count` minus the number of slots
    /// effected by this shift operation.
    /// * `shifted_slot` - The value in the highest-most slot in this block, which due to the shift has
    /// overflowed the block and should be placed into the next block if the shift operation is
    /// going to continue.  This can be pased back in to shift_slots as the
    /// `previous_shifted_value` argument to ensure it's placed in the next block in the right
    /// place.
    ///
    /// # TODO
    ///
    /// It turns out this return value with the remaining_slot_count isn't needed since the caller
    /// needs to keep track of this anyway for other reasons.  Can remove it and just return the
    /// previously shifted slot value.
    #[inline]
    #[allow(unused_variables, dead_code)]
    pub fn shift_slots_left(
        &mut self,
        rbits: usize,
        start_relative_index: usize,
        slot_count: usize,
        previous_shifted_slot: u64,
    ) -> (usize, u64) {
        debug_assert!(start_relative_index < SLOTS_PER_BLOCK);
        debug_assert!(slot_count > 0);
        debug_assert!((previous_shifted_slot & !bitmask!(rbits)) == 0);

        let (remaining_slot_count, shifted_value) = self.slots.shift_slots_left(
            rbits,
            start_relative_index,
            slot_count,
            previous_shifted_slot,
        );

        (remaining_slot_count, shifted_value)
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

    /// Given the relative index of a slot, checks the `occupies` bitmap and returns the number of
    /// `1` bits up to `relative_index`
    #[inline]
    pub fn get_occupied_rank(&self, relative_index: usize) -> usize {
        self.occupieds.popcnt_first_n(relative_index)
    }

    /// Given the relative index of a slot, checks the `occupies` bitmap and returns the number of
    /// `1` bits up to `relative_index`.  Skips the first `skip_count` bits in `occupieds` before
    /// computing the rank
    #[inline]
    pub fn get_occupied_rank_skip_n(&self, skip_count: usize, relative_index: usize) -> usize {
        debug_assert!(relative_index >= skip_count, 
                      "skip_count is {} but relative_index is {} which will always return 0; are you sure this is what you meant?",
                      skip_count, relative_index);
        (self.occupieds & !bitmask!(skip_count)).popcnt_first_n(relative_index)
    }

    #[inline]
    pub fn count_occupieds(&self, relative_index: usize, bit_count: usize) -> usize {
        assert!(bit_count > 0);
        debug_assert!(relative_index < SLOTS_PER_BLOCK);
        debug_assert!(bit_count <= SLOTS_PER_BLOCK);
        debug_assert!(relative_index + bit_count <= SLOTS_PER_BLOCK);

        self.occupieds.count_set_bits(relative_index, bit_count)
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

    #[inline]
    pub fn count_runends(&self, relative_index: usize, bit_count: usize) -> usize {
        assert!(bit_count > 0);
        debug_assert!(relative_index < SLOTS_PER_BLOCK);
        debug_assert!(bit_count <= SLOTS_PER_BLOCK);
        debug_assert!(relative_index + bit_count <= SLOTS_PER_BLOCK);

        self.runends.count_set_bits(relative_index, bit_count)
    }

    /// Scans through this block looking for the `rank`-th set runend bit.  If it finds it, returns
    /// that bit.  If it doesnt, returns how many set `runend` bits it passed along the way so the
    /// scan can continue at the next block.
    ///
    /// # Arguments
    ///
    /// * `skip_count` - The number of runend bits (set or not doesn't matter) to skip before
    /// starting the scan.  `0` means the entire `runends` field is scanned.
    /// * `rank` - The rank of the runend bit to find.  Rank in this case means the `rank`-th set
    /// bit in the `runends` field (after skipping `skip_count` initial bits)
    ///
    /// # Returns
    ///
    /// * `FoundIt(x)` - If the desired runend is found, returns its relative index in `x`
    /// * `KeepScanning(x)` - If there is no runend of the desired rank, returns the number of set
    /// runend bits that were found in this block in `x`.  The caller can use this to presume the
    /// scan in the next block, minus the number of runends that were found in this one.
    #[inline]
    pub fn scan_for_runend(&self, skip_count: usize, rank: usize) -> ScanForRunendResult {
        match self.runends.bitselect_skip_n(skip_count, rank) {
            Some(n) => {
                //Found the bit of the desired rank!
                ScanForRunendResult::FoundIt(n)
            }
            None => {
                //Did not find it, but check how many were set
                let runend_count = self.runends.popcnt_skip_n(skip_count);
                ScanForRunendResult::KeepScanning(runend_count)
            }
        }
    }

    /// Shifts the runends bitmap to the left, starting at `start_relative_index`-th bit, with
    /// `bit_count` bits included in the shift.  This works the same in concept as
    /// `shift_slots_left` except that method operates on the slots in the block and this method
    /// operates on the bits in the `runends` bitmap each of which corresponds to a slot.
    ///
    /// In normal use, slots and runends are shifted in sync, though that's not enforced at this
    /// level.
    ///
    /// # Returns
    ///
    /// `shifted_bit` - the value of the `runends` bit at `start_relative_index+bit_count-1` which
    /// was overwritten by this shift operation.  This allows the caller to shift across multiple
    /// blocks by shifting a lower block and then passing this `shifted_bit` return value into the
    /// next block's shift operation as `previous_shifted_bit`
    #[inline]
    pub fn shift_runends_left(
        &mut self,
        start_relative_index: usize,
        bit_count: usize,
        previous_shifted_bit: bool,
    ) -> bool {
        let (new_runends, shifted_bit) = self.runends.shift_left_into_partial(
            start_relative_index, //bit index at which to start the shift (`previous_shifted_bit` is inserted here)
            bit_count, //number of bits to include in the shift operation.  the bit at `start_relative_index+bit_count-1` is overwritten
            1, //number of bits to shift by; just one bit in this case we're inserting a runend bit
            if previous_shifted_bit { 0x01 } else { 0x00 }, //convert `previous_shifted_bit` into a u64 to insert at `start_relative_index`
        );

        self.runends = new_runends;

        if shifted_bit == 0 {
            false
        } else {
            true
        }
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
            block.set_occupied(i, true);
        }

        assert_eq!(0xffff_ffff_ffff_ffffu64, block.occupieds);
        assert_eq!(0x0, block.runends);

        for i in 0..SLOTS_PER_BLOCK {
            block.set_runend(i, true);
        }

        assert_eq!(0xffff_ffff_ffff_ffffu64, block.occupieds);
        assert_eq!(0xffff_ffff_ffff_ffffu64, block.runends);

        block.occupieds = 0;
        block.runends = 0;

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

    #[test]
    pub fn get_occupied_rank_tests() {
        let mut block: Block = Block {
            ..Default::default()
        };

        // All occupied bits are empty now so all slots have the same rank
        for i in 0..SLOTS_PER_BLOCK {
            assert_eq!(0, block.get_occupied_rank(i));
        }

        // Set all the bits and verify they have the expected ranks
        // the rank computation is inclusive of the index, so an index of 0
        // will return 1 if the first bit is set
        block.occupieds = std::u64::MAX;

        for i in 0..SLOTS_PER_BLOCK {
            assert_eq!(i + 1, block.get_occupied_rank(i));
        }

        // Verify the correct behavior for the variant that skips some number of bits before
        // starting to count
        for i in 0..SLOTS_PER_BLOCK {
            for j in i..SLOTS_PER_BLOCK {
                assert_eq!(j - i + 1, block.get_occupied_rank_skip_n(i, j));
            }
        }
    }

    #[test]
    pub fn count_occupieds_tests() {
        let mut block: Block = Block {
            ..Default::default()
        };

        assert_eq!(0, block.count_occupieds(0, SLOTS_PER_BLOCK));

        block.set_occupied(1, true);

        assert_eq!(0, block.count_occupieds(0, 1));
        assert_eq!(1, block.count_occupieds(0, 2));
        assert_eq!(1, block.count_occupieds(0, SLOTS_PER_BLOCK));
        assert_eq!(1, block.count_occupieds(1, 1));
        assert_eq!(1, block.count_occupieds(1, SLOTS_PER_BLOCK - 1));
        assert_eq!(0, block.count_occupieds(2, SLOTS_PER_BLOCK - 2));

        //Set all occupied bits and play with how many are counted
        block.occupieds = std::u64::MAX;

        assert_eq!(1, block.count_occupieds(0, 1));
        assert_eq!(2, block.count_occupieds(0, 2));
        assert_eq!(3, block.count_occupieds(0, 3));
        assert_eq!(4, block.count_occupieds(0, 4));
        assert_eq!(SLOTS_PER_BLOCK, block.count_occupieds(0, SLOTS_PER_BLOCK));
        assert_eq!(
            SLOTS_PER_BLOCK - 1,
            block.count_occupieds(0, SLOTS_PER_BLOCK - 1)
        );
        assert_eq!(
            SLOTS_PER_BLOCK - 2,
            block.count_occupieds(0, SLOTS_PER_BLOCK - 2)
        );
    }

    #[test]
    pub fn scan_for_runend_tests() {
        // test cases for the scan_for_runend function.
        //
        // Layout is:
        //
        // (runends_value, skip_count, rank, expected_result)
        let test_data = [
            (0u64, 0, 0, ScanForRunendResult::KeepScanning(0)),
            (0x8000_8001u64, 0, 0, ScanForRunendResult::FoundIt(0)),
            (0x8000_8001u64, 1, 0, ScanForRunendResult::FoundIt(15)),
            (0x8000_8001u64, 2, 0, ScanForRunendResult::FoundIt(15)),
            (0x8000_8001u64, 15, 0, ScanForRunendResult::FoundIt(15)),
            (0x8000_8001u64, 16, 0, ScanForRunendResult::FoundIt(31)),
            (0x8000_8001u64, 31, 0, ScanForRunendResult::FoundIt(31)),
            (0x8000_8001u64, 32, 0, ScanForRunendResult::KeepScanning(0)),
            (0x8000_8001u64, 0, 1, ScanForRunendResult::FoundIt(15)),
            (0x8000_8001u64, 0, 2, ScanForRunendResult::FoundIt(31)),
            (0x8000_8001u64, 0, 3, ScanForRunendResult::KeepScanning(3)),
            (0x8000_8001u64, 1, 3, ScanForRunendResult::KeepScanning(2)),
            (0x8000_8001u64, 16, 3, ScanForRunendResult::KeepScanning(1)),
            (0x8000_8001u64, 32, 3, ScanForRunendResult::KeepScanning(0)),
            (0xffff_ffffu64, 0, 0, ScanForRunendResult::FoundIt(0)),
            (0xffff_ffffu64, 1, 0, ScanForRunendResult::FoundIt(1)),
            (0xffff_ffffu64, 2, 0, ScanForRunendResult::FoundIt(2)),
            (0xffff_ffffu64, 31, 0, ScanForRunendResult::FoundIt(31)),
            (0xffff_ffffu64, 32, 0, ScanForRunendResult::KeepScanning(0)),
        ];

        for (runends_value, skip_count, rank, expected_result) in &test_data {
            let block = Block {
                runends: *runends_value,
                ..Default::default()
            };

            assert_eq!(
                *expected_result,
                block.scan_for_runend(*skip_count, *rank),
                "scan_for_runend({}, {}) with runends value {:016x} mismatched",
                skip_count,
                rank,
                runends_value
            );
        }
    }
}
