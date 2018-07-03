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

/// The type which represents the value of a slot.  Note that the actual number of bits stored
/// and retrieved is not 64 but is determiend by the `rbits` runtime parameter.
type SlotValue = u64;

#[allow(dead_code)] // just for development
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
    pub fn get_slot(&self, index: usize) -> SlotValue {
        let block_index = index / block::SLOTS_PER_BLOCK;
        let relative_index = index % block::SLOTS_PER_BLOCK;

        assert!(
            block_index < self.blocks.len(),
            "slot index {} is out of range",
            index
        );

        self.blocks[block_index].get_slot(self.rbits, relative_index)
    }

    /// Sets the value currently stored in the `index`th slot.  Note that this isn't necessarily
    /// the value corresponding to any specific quotient from which `index` was derived; this is
    /// just an array lookup.
    ///
    /// # Returns
    ///
    /// Returns the previous value stored in the slot.
    pub fn set_slot(&mut self, index: usize, val: SlotValue) -> SlotValue {
        let block_index = index / block::SLOTS_PER_BLOCK;
        let relative_index = index % block::SLOTS_PER_BLOCK;

        assert!(
            block_index < self.blocks.len(),
            "slot index {} is out of range",
            index
        );

        self.blocks[block_index].set_slot(self.rbits, relative_index, val)
    }

    /// Given an quotient `index` which identifies a slot containining a run (note: not necessarily the run corresponding
    /// to the quotient `index`; it could contain a run for an lower quotient value if values have
    /// shifted), determines the slot index where that run ends.
    #[inline]
    pub fn find_run_end(&self, index: usize) -> usize {
        //This corresponds to the cqf.c function run_end
        let slot_block_index = index / block::SLOTS_PER_BLOCK;
        let slot_intrablock_index = index % block::SLOTS_PER_BLOCK;
        let block_offset = self.get_block_offset(slot_block_index);

        // within this block, find the rank of `index` slot's bit in the `occupied` bitmap.
        // Note that this `occupied` bitmap only covers the range of 64 slots in this block it's
        // not global
        //
        // Recall that "rank" means the number of 1 buts up to the specified bit (inclusive)
        let slot_intrablock_rank =
            self.blocks[slot_block_index].get_occupied_rank(slot_intrablock_index);
        if slot_intrablock_rank == 0 {
            //No occupied slots anywhere in this block from the start up to slot_intrablock_index.
            //In most cases this means the slot at `index` isn't occupied, but it could also mean
            //the block we're looking in doesn't contain that slot's value due to being shifted
            if block_offset <= slot_intrablock_index {
                //This is the normal case.  It means we have the correct block, there hasn't been
                //any shifting around.  We confirmed that the rank of this slot in `occupieds` is 0
                //so there are no runs in this block.  That means the 'run end' for this index is
                //itself
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

    // TODO: Implement may_contain in terms of the blocks.  Need to figure out
    // how to use `offset` to ensure we're testing the right block's bitfields

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
