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
    pub rbits: usize,
}

/// The type which represents the value of a hash.  Though this is the same underlying type as
/// `SlotValue`, semantically they mean different things.  `SlotValue` is the `rbits`-bits long
/// value of the "remainder" portion of a hash value. `HashValue` by contrast is the entire 64-bit
/// hash value, which we logically divide into the `rbits`-bit "remainder", the `qbits`-bit
/// "quotient", and if there are any bits left over we discard them.
///
/// This type, as well as `RemainderValue` and `QuotientValue`, all have the same integer type,
/// they are separated out as separate type aliases to make it more clear which kind of value is
/// being used where.
///
/// The actual C code RSQF implementation makes this less clear, and sometimes uses terms like
/// `index`, `hash_value`, `slot` interchangeably.
#[allow(dead_code)] // for now
pub type HashValue = u64;

/// The type which represents the value of the remainder portion of a `HashValue`.  It is these
/// remainders which are stored in `rbits`-bit slots within the physical data layer.  
/// Note that the actual number of bits stored and retrieved is not 64 but is determiend by the `rbits` runtime parameter.
pub type RemainderValue = u64;

/// The type which contains the "quotient" part of a `HashValue`.  The number of bits used to
/// actually represent a quotient is computed in higher layers as the parameter `q` or `qbits`.
pub type QuotientValue = u64;

#[allow(dead_code)] // just for development
#[allow(unused_variables)]
impl PhysicalData {
    /// Creates a new physical data structure for storing remainders, where each remainder is
    /// `rbits` bits long and a total of `nslots` slots are stored.
    ///
    /// # Remarks
    ///
    /// Internally slots are stored in blocks of 64 slots at a time.  So if `rbits` is not an
    /// integer multiple of 64, the actual size of the structure returned will be rounded up.
    ///
    /// Also note that in this prototype version, though `rbits` is parameterized, the only
    /// supported value is `block::BITS_PER_SLOT`.  Any other value causes a runtime assertion.
    pub fn new(nslots: usize, rbits: usize) -> PhysicalData {
        assert!(rbits > 0);

        let nblocks = (nslots + block::SLOTS_PER_BLOCK - 1) / block::SLOTS_PER_BLOCK;

        // Allocate a vector of Block structures.  Because of how this structure will be used,
        // the contents should be populated in advance
        let blocks = vec![Default::default(); nblocks];
        PhysicalData { blocks, rbits }
    }

    /// Gets the length of this structure in terms of the number of slots
    pub fn len(&self) -> usize {
        self.blocks.len() * block::SLOTS_PER_BLOCK
    }

    /// Gets the value currently stored in the `index`th slot.  Note that this isn't necessarily
    /// the value corresponding to any specific quotient from which `index` was derived; this is
    /// just an array lookup.
    pub fn get_slot(&self, index: usize) -> RemainderValue {
        let (block_index, relative_index) = self.get_slot_location(index);

        self.blocks[block_index].get_slot(self.rbits, relative_index)
    }

    /// Sets the value currently stored in the `index`th slot.  Note that this isn't necessarily
    /// the value corresponding to any specific quotient from which `index` was derived; this is
    /// just an array lookup.
    ///
    /// # Returns
    ///
    /// Returns the previous value stored in the slot.
    pub fn set_slot(&mut self, index: usize, val: RemainderValue) -> RemainderValue {
        let (block_index, relative_index) = self.get_slot_location(index);

        self.blocks[block_index].set_slot(self.rbits, relative_index, val)
    }

    /// Tests if a given slot's "runend" flag is set.  
    pub fn is_runend(&self, index: usize) -> bool {
        let (block_index, relative_index) = self.get_slot_location(index);
        self.blocks[block_index].is_runend(relative_index)
    }

    /// Sets the runend bit for the specified index.
    pub fn set_runend(&mut self, index: usize, val: bool) -> () {
        let (block_index, relative_index) = self.get_slot_location(index);
        self.blocks[block_index].set_runend(relative_index, val)
    }

    /// Tests if a given slot's "occupied" flag is set.  Note this doesn't mean that specific slot
    /// has data in it, it means the quotient with that slot as it's home slot is present somewhere
    /// in the filter.
    pub fn is_occupied(&self, index: usize) -> bool {
        let (block_index, relative_index) = self.get_slot_location(index);
        self.blocks[block_index].is_occupied(relative_index)
    }

    /// Sets the occupied bit for the specified index.
    pub fn set_occupied(&mut self, index: usize, val: bool) -> () {
        let (block_index, relative_index) = self.get_slot_location(index);
        self.blocks[block_index].set_occupied(relative_index, val)
    }

    /// Given an quotient `index` which identifies a slot containining a run (note: not necessarily the run corresponding
    /// to the quotient `index`; it could contain a run for an lower quotient value if values have
    /// shifted), determines the slot index where that run ends.
    #[inline]
    pub fn find_run_end_shitty(&self, index: usize) -> usize {
        //This corresponds to the cqf.c function run_end
        let (slot_block_index, slot_intrablock_index) = self.get_slot_location(index);
        let block_offset = self.get_block_offset(slot_block_index);

        // within this block, find the rank of `index` slot's bit in the `occupied` bitmap.
        // Note that this `occupied` bitmap only covers the range of 64 slots in this block it's
        // not global
        //
        // Recall that "rank" means the number of 1 buts up to the specified bit (inclusive)
        let slot_intrablock_rank =
            self.blocks[slot_block_index].get_occupied_rank(slot_intrablock_index);
        if slot_intrablock_rank == 0 {
            // the quotient whose 'home slot' is `index` is not stored in the filter, as we can
            // tell from the `occupieds` bitmap that we just tested.  It's still possible that
            // `index` is being used to store remainders in a run from a lower quotient value...
            if block_offset <= slot_intrablock_index {
                // `block_offset` is how far from slot 0 of this block is the run end corresponding
                // to that slot 0.  It's less than the index of this slot in the block.
                // The C code uses this to determine that the run end for `index` is itself.
                // I can't explain that.  What if this is the 63rd slot in a block, and between
                // `block_offset` and this slot is the start of another run that goes on for
                // multiple blocks?
                return index;
            } else {
                //THis is the less common case.
                //
                //the `block_offset` tells us how far from the first slot in the block, which we'll
                //call `i`, is the run end for the run which contains `i`.  Every 64 slots has this
                //pre-compute offset; for the other 63 slots in a block we must compute it from the
                //one that is pre-computed.  If `block_offset` is greater than
                //`slot_intrablock_index` it means that there is a run which includes all of the
                //slots in this block up to slot_intrablock_index and beyond.  But remember, in
                //this method we're trying to find where the end of the run containing this slot
                //is, and this `block_offset` has helpfully told us:
                return block::SLOTS_PER_BLOCK * slot_block_index + block_offset - 1;
            }
        }

        //The easy path has been exhausted; there are definitely SOME occupied slots in this block
        //up to or including the slot we're asking about.  Need to figure out where the run
        //correspondign to our particular slot is, and thereby where the run ends.
        panic!("NYI");
    }

    /// Given a quotient value `quotient` (in the paper this is `h0(x)` or sometimes `q`) attempts
    /// to find the slot index where that quotient's run of remainder values ends.  If this
    /// quotient is not "occupied" (meaning no remainders for that quotient have been stored in the
    /// filter) then the result is None.
    #[inline]
    pub fn find_quotient_run_end(&self, quotient: QuotientValue) -> Option<usize> {
        //Find the block where this quotient is located, then check the `occupieds` field for this
        //quotient's bit.  If that bit is not set, then there are no runs for this quotient.
        let (block_index, intrablock_index) = self.get_slot_location(quotient as usize);
        let block = self.get_block(block_index);

        if !block.is_occupied(intrablock_index) {
            //This quotient is not occupied at all so there is no run end.
            return None;
        }

        //Now we know the quotient has at least one and possibly multiple remainders.  The question
        //is where.  For filters that are mostly empty, it's likely this quotient's run of
        //remainders starts at the quotient's home slot, but as the filter fills up that becomes
        //less likely.  We will start by looking at the block containing this quotient's home slot.
        //Remember that one invariant of this kind of filter is that the run for a quotient starts
        //either at its home slot, or if that's in use the first free slot after the home slot.
        //That narrows down our search considerably.
        //
        //To avoid having to jump around too many places, use the `offset` hint at the start of
        //this block, which tells us where the runend is for the first slot in that block.
        //On the off chance the slot we're looking for IS the first slot in the block that's easy
        //but even if not we can start the search there.
        let block_runend_offset = self.get_block_offset(block_index);

        //Get the rank of this slot's occupieds bit relative to the start of this block.
        //This computation correponds to step 1 in figure 2 in the RDSF paper.
        let relative_rank = block.get_occupied_rank(intrablock_index);

        //We know this slot is occupied, so there the rank of its corresponding occupieds bit
        //should be at least 1 or something's gone dreadfully wrong
        debug_assert!(
            relative_rank > 0,
            "quotient {} in block {} slot {} occupieds bit relative rank is 0",
            quotient,
            block_index,
            intrablock_index
        );

        //Find the runend bit with rank `relative_rank` where that rank is relative to the runend
        //for the slot 0 of this block.
        let mut block = block;
        let mut current_block_index = block_index + (block_runend_offset / block::SLOTS_PER_BLOCK);
        let mut skip_bits = block_runend_offset % block::SLOTS_PER_BLOCK;
        let mut runend_rank = relative_rank - 1;

        loop {
            // scan for the run end AFTER the block slot 0 runend, up to the rank we computed for
            // the occupieds bit.  This corresponds to step 2 in figure 2 of the RSDF paper.
            match block.scan_for_runend(skip_bits, runend_rank) {
                block::ScanForRunendResult::FoundIt(runend_index) => {
                    //Found the block we were looking for at the specified index!
                    //This expressoin corresponds to step 3 of figure 2 of the RSDF paper
                    return Some(current_block_index * block::SLOTS_PER_BLOCK + runend_index);
                }
                block::ScanForRunendResult::KeepScanning(skipped_runends) => {
                    //The runend of the desired rank was not found.  It means the runend we're
                    //looking for must be in the next block.  Reduce the rank we're looking for
                    //based on the number we've skipped, and loop again
                    current_block_index += 1;
                    skip_bits = 0;
                    runend_rank -= skipped_runends;
                    block = self.get_block(current_block_index);
                }
            }
        }
    }

    #[inline]
    /// Given a range of slots from `home_slot` up to and including `run_end`, scans backwards from
    /// `run_end` looking for `remainder`.  Stops after checking `home_slot` or upon reaching a
    /// slot with its `runends` bit set.
    ///
    /// # Returns
    ///
    /// `Some(index)` where `index` is the slot where `remainder` was found, or...
    /// `None` if `remainder` is not found
    pub fn scan_for_remainder(
        &self,
        run_end: usize,
        home_slot: usize,
        remainder: RemainderValue,
    ) -> Option<usize> {
        // we assume home_slot comes at or before run_end
        debug_assert!(
            home_slot <= run_end,
            "home_slot 0x{:x} cannot be after run_end 0x{:x}!",
            home_slot,
            run_end
        );
        // we are assuming that `remainder` doesn't have any values in the bits higher than `rbits`
        debug_assert!(
            remainder & !bitmask!(self.rbits) == 0,
            "remainder 0x{:x} has bits set above {}",
            remainder,
            self.rbits
        );
        let (mut block_index, mut intrablock_index) = self.get_slot_location(run_end);
        let (home_block_index, home_intrablock_index) = self.get_slot_location(home_slot);
        let mut block = self.get_block(block_index);

        loop {
            if block.get_slot(self.rbits, intrablock_index) == remainder {
                return Some(block_index * block::SLOTS_PER_BLOCK + intrablock_index);
            }

            //Not found at this slot.
            //Stop the search if this was the home slot
            if block_index == home_block_index && intrablock_index == home_intrablock_index {
                return None;
            }

            //Move back one slot
            if intrablock_index > 0 {
                intrablock_index -= 1;
            } else {
                block_index -= 1;
                block = self.get_block(block_index);
                intrablock_index = block::SLOTS_PER_BLOCK;
            }

            //Stop if we've reached another run end, otherwise keep scanning
            if block.is_runend(intrablock_index) {
                return None;
            }
        }
    }

    /// Given the index for some slot `index`, assuming that slot is part of some run, figures out
    /// the index of the run end (that is, the last slot that is part of the same run) for that
    /// slot.
    ///
    /// Importantly, this is diffrent from the `find_quotient_run_end` above in that the
    /// `find_quotient_run_end` specifies a specific quotient of interest, and finds the run for
    /// that quotient wherever in the slots it is, then computes its run end.  This method doesn't
    /// care what quotient any of the runs correspond to; this slot is presumed to be part of a
    /// run, and it finds that end.  Higher level code that this is needed to figure out which
    /// quotient this run belongs to.
    #[inline]
    pub fn find_slot_run_end(&self, index: usize) -> Option<usize> {
        panic!("NYI");
    }

    /// Given the index of a block, return that block's offset value.  Unlike the name implies,
    /// it's not the offset of the block, it's a pre-computed value of the distance between the
    /// first slot in the block (let's call it `i`) and the run-end for whatever run contains `i`,
    /// or 0 if `i` is unused.
    #[inline]
    fn get_block_offset(&self, block_index: usize) -> usize {
        // corresponds to the cqf.c function block_offset
        match self.blocks[block_index].offset() {
            Some(o) => o,
            None => {
                //The offset value for this block has exceeded the maximum capacity of the u8 type
                //so it means we must traverse more of the structure to determine the actual
                //offset.
                //
                //the block_offset function in the C code handles this by calling into `run_end`
                //but I don't understand why that complexity is necessary; can't we try the next
                //block over?  I'm doing it the way I think will work and maybe I'll have to change
                //it once I understand why another approach is needed
                let mut i = 1usize;
                loop {
                    if let Some(o) = self.blocks[block_index + i].offset() {
                        return i * block::SLOTS_PER_BLOCK + o;
                    } else {
                        //TODO: an optimization could be to skip ahead by more than one block.
                        //if the offset overflows it's as 255, which means the highest valid value
                        //is 254.  So we could safely skip ahead three blocks at a time (because
                        //3*64=192, but 4*64=256).
                        i = i + 1;
                        assert!(
                            block_index + i < self.blocks.len(),
                            "Could not find the block offfset before the end for block index {}",
                            block_index
                        );
                    }
                }
            }
        }
    }

    /// Given the index of a slot, computes not the actual offset of its run end, but the lower
    /// bound on that offset.  I don't understand why this is used, it comes from the
    /// `offset_lower_bound` function in the C implementation.  Perhaps this is more efficient than
    /// computing the actual end of the run?
    #[inline]
    fn get_slot_offset_lower_bound(&self, index: usize) -> usize {
        let (slot_block_index, slot_intrablock_index) = self.get_slot_location(index);
        self.blocks[slot_block_index].get_slot_offset_lower_bound(slot_intrablock_index)
    }

    /// Gets an immutable reference to a block.  Using Rust lifetime trickery ensures this doesn't
    /// result in a dangling pointer.
    #[inline]
    fn get_block<'s: 'block, 'block>(&'s self, index: usize) -> &'block block::Block {
        debug_assert!(
            index < self.blocks.len(),
            "block index {} is out of bounds",
            index
        );

        &self.blocks[index]
    }

    /// Does the repetitive and simple math to translate from a slot index to a block index and the
    /// index within that block where the slot is located.
    #[inline]
    fn get_slot_location(&self, index: usize) -> (usize, usize) {
        let block_index = index / block::SLOTS_PER_BLOCK;
        let intrablock_index = index % block::SLOTS_PER_BLOCK;

        (block_index, intrablock_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_RBITS: usize = block::BITS_PER_SLOT;
    const TEST_BLOCKS: usize = 1024;
    const TEST_SLOTS: usize = block::SLOTS_PER_BLOCK * TEST_BLOCKS;

    #[test]
    fn creates_blocks() {
        let pd = PhysicalData::new(TEST_SLOTS, TEST_RBITS);

        assert_eq!(pd.rbits, TEST_RBITS);
        assert_eq!(pd.len(), TEST_SLOTS);
        assert_eq!(pd.blocks.len(), TEST_BLOCKS);
        assert_eq!(pd.blocks[0], Default::default());
    }

    #[test]
    fn rounds_up_to_nearest_whole_block() {
        let pd = PhysicalData::new(TEST_SLOTS - 1, TEST_RBITS);

        // Even though we specified one less slot, it rounded up
        assert_eq!(pd.len(), TEST_SLOTS);
        assert_eq!(pd.blocks.len(), TEST_BLOCKS);
    }

    #[test]
    #[should_panic]
    fn set_out_of_range_panics() {
        let mut pd = PhysicalData::new(TEST_SLOTS, TEST_RBITS);

        pd.set_slot(TEST_SLOTS, 0);
    }

    #[test]
    #[should_panic]
    fn get_out_of_range_panics() {
        let pd = PhysicalData::new(TEST_SLOTS, TEST_RBITS);

        pd.get_slot(TEST_SLOTS);
    }

    #[test]
    fn get_set_work_properly() {
        let mut pd = PhysicalData::new(TEST_SLOTS, TEST_RBITS);

        // first go through each slot and set it to a value derived from its index
        for index in 0..TEST_SLOTS {
            let slot_value = (index as u64) & bitmask!(pd.rbits);

            assert_eq!(0, pd.get_slot(index));
            assert_eq!(0, pd.set_slot(index, slot_value));
            assert_eq!(slot_value, pd.get_slot(index));
        }

        for index in 0..TEST_SLOTS {
            let slot_value = (index as u64) & bitmask!(pd.rbits);

            assert_eq!(slot_value, pd.get_slot(index));
            assert_eq!(slot_value, pd.set_slot(index, 0));
            assert_eq!(0, pd.get_slot(index));
        }
    }

    #[test]
    pub fn find_quotient_run_end_test() {
        let mut pd = PhysicalData::new(TEST_SLOTS, TEST_RBITS);

        //Empty data set, there are no runs so no run ends
        for q in 0..TEST_SLOTS {
            assert_eq!(None, pd.find_quotient_run_end(q as QuotientValue));
        }

        let q = 100usize;

        //Simulate a single run in a single slot.
        {
            pd.set_occupied(q, true);
            pd.set_runend(q, true);

            assert_eq!(
                Some(q as usize),
                pd.find_quotient_run_end(q as QuotientValue)
            );
        }

        //Now let's make not a single slot but a longer run
        {
            pd.set_runend(q, false);
            pd.set_runend(q + 5, true);

            assert_eq!(
                Some(q as usize + 5),
                pd.find_quotient_run_end(q as QuotientValue)
            );
        }

        // clean up the bits
        pd.set_occupied(q, false);
        pd.set_runend(q, false);

        //Let's test another corner case; the run is exactly on the start of a block.  In that case
        //we will use the block's `offset` value as a shortcut to spare the effort of looking at
        //runends.
        let q = 5 * block::SLOTS_PER_BLOCK;
        pd.blocks[q / block::SLOTS_PER_BLOCK].set_offset(13);
        pd.set_occupied(q, true);
        pd.set_runend(q + 13, true);

        assert_eq!(Some(q + 13), pd.find_quotient_run_end(q as QuotientValue));

        //Simulate a pathological edge case: every slot is occupied and contains a single-slot run.
        pd.blocks[q / block::SLOTS_PER_BLOCK].set_offset(0);
        for q in 0..TEST_SLOTS {
            pd.set_occupied(q, true);
            pd.set_runend(q, true);
        }

        for q in 0..TEST_SLOTS {
            assert_eq!(
                Some(q),
                pd.find_quotient_run_end(q as QuotientValue),
                "slot {} should be its own run end but it's not",
                q
            );;
        }
    }

    #[test]
    pub fn find_quotient_run_end_torture_test() {
        //Test all possible variations of occupied/runend configuration where only one run exists
        //in the filter
        const NSLOTS: usize = 512; //this is too exhaustive a test to use the normal test size
        let mut pd = PhysicalData::new(NSLOTS, TEST_RBITS);
        for occupied in 0..NSLOTS {
            pd.set_occupied(occupied, true);

            for runend in occupied..NSLOTS {
                pd.set_runend(runend, true);

                assert_eq!(
                    Some(runend),
                    pd.find_quotient_run_end(occupied as QuotientValue),
                    "occupied at slot {}, runend at slot {}",
                    occupied,
                    runend
                );

                //clean up runend
                pd.set_runend(runend, false);
            }

            //clean up occupied
            pd.set_occupied(occupied, false);
        }
    }

    #[test]
    pub fn scan_for_remainder_tests() {
        let mut pd = PhysicalData::new(TEST_SLOTS, TEST_RBITS);
        const REMAINDER: RemainderValue = 0x1fe;
        const REMAINDER_SLOT: usize = 100;

        //Scan should stop at the home slot if not found
        //Since the structure is empty now nothing will ever be found
        assert_eq!(None, pd.scan_for_remainder(TEST_SLOTS - 1, 0, REMAINDER));

        //Insert the remainder value in some place before the home_slot to make sure its not found
        pd.set_slot(REMAINDER_SLOT, REMAINDER);
        assert_eq!(
            None,
            pd.scan_for_remainder(TEST_SLOTS - 1, REMAINDER_SLOT + 1, REMAINDER)
        );

        //But if we move the home slot back one, to the slot containing the remainder, it should be
        //found
        assert_eq!(
            Some(REMAINDER_SLOT),
            pd.scan_for_remainder(TEST_SLOTS - 1, REMAINDER_SLOT, REMAINDER)
        );

        //Now set a runend bit somewhere where the remainder is located.  That
        //should also cause the stop without finding the remainder
        pd.set_runend(REMAINDER_SLOT, true);
        assert_eq!(
            None,
            pd.scan_for_remainder(TEST_SLOTS - 1, REMAINDER_SLOT, REMAINDER)
        );
    }

    #[test]
    pub fn get_offset_test() {
        let mut pd = PhysicalData::new(TEST_SLOTS, TEST_RBITS);

        //Initially, all blocks have zero offset
        for index in 0..(TEST_SLOTS / block::SLOTS_PER_BLOCK) {
            assert_eq!(0, pd.get_block_offset(index));
        }

        //Create a situation in which there's a long run spanning multiple blocks
        let run_length = TEST_SLOTS - 1;
        for index in 0..(TEST_SLOTS / block::SLOTS_PER_BLOCK) {
            pd.blocks[index].set_offset(run_length - (index * block::SLOTS_PER_BLOCK));
        }

        for index in 0..(TEST_SLOTS / block::SLOTS_PER_BLOCK) {
            assert_eq!(
                run_length - (index * block::SLOTS_PER_BLOCK),
                pd.get_block_offset(index),
            );
        }
    }
}
