//! `PhysicalData` contains the actual in-memory data structures which represent the RSQF.  It
//! tries to abstract away most of the bit-fiddling trickery and expose a higher-level interface
//! that might be understandable by someone who's read the RSQF paper but doesn't squint at
//! low-level C code for a living
use block;
use std::vec::Vec;

#[allow(dead_code)] // for now
#[derive(Default, PartialEq, Clone)]
pub struct PhysicalData {
    blocks: Vec<block::Block>,
    pub rbits: usize,
    nslots: usize,
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

/// When operating on a range of slots, the operations are performed on a block-by-block basis.
/// This iterator exposes an arbitrary slot range as an iterator that yields the block index and
/// range in that block for every block that contains a part of the slot range
#[derive(Debug, Copy, Clone)]
struct SlotRangeIterator {
    start_block_index: usize,
    start_intrablock_index: usize,
    end_block_index: usize,
    end_intrablock_index: usize,
    next_block_index: Option<usize>,
}

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
        PhysicalData {
            blocks,
            rbits,
            nslots: nblocks * block::SLOTS_PER_BLOCK,
        }
    }

    /// Gets the length of this structure in terms of the number of slots
    pub fn len(&self) -> usize {
        self.nslots
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

    /// Shifts a portion of the `runends` bitmap left by one bit and inserts the bit `insert_val`
    /// in the resulting space.  See `shift_slots_left` for more details; this method does for
    /// `runends` bits what `shift_slots_left` does for slots.
    #[inline]
    pub fn shift_runends_left(&mut self, start_index: usize, bit_count: usize, insert_val: bool) {
        let mut shifted_val = insert_val;

        for (block_index, start_intrablock_index, slot_length) in
            self.get_slot_range_iter(start_index, bit_count)
        {
            let block = self.get_block_mut(block_index);
            shifted_val = block.shift_runends_left(start_intrablock_index, slot_length, shifted_val)
        }
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
    pub fn find_run_end(&self, index: usize) -> usize {
        //This corresponds to the cqf.c function run_end
        let (block_index, intrablock_index) = self.get_slot_location(index);
        let block = self.get_block(block_index);
        let block_offset = self.get_block_offset(block_index);

        // within this block, find the rank of `index` slot's bit in the `occupied` bitmap.
        // Note that this `occupied` bitmap only covers the range of 64 slots in this block it's
        // not global
        //
        // Recall that "rank" means the number of 1 buts up to the specified bit (inclusive).  So
        // the question being asked by this operation is "how many occupied slots are there in this
        // block up to and including `slot_block_index`?
        let intrablock_rank = block.get_occupied_rank(intrablock_index);
        if intrablock_rank == 0 {
            //the rank of this index in the `occupieds` bitmap is 0, that means there are no
            //occupied slots in this block up to and includng `slot_intrablock_index`.
            //
            //This could mean that slot `slot_intrablock_index` is unused completely, or it could
            //mean it's used as part of a run that started prior to this block.  (If it were part
            //of a run that started in THIS block, there would be at least one `occupieds` bit set
            //but recall the rank is 0).
            if block_offset <= intrablock_index {
                // `block_offset` is how far from slot 0 of this block is the run end corresponding
                // to that slot 0.  It's less than the index of this slot in the block.  That means
                // if there is a run which starts in a previous block and ends in this one, it ends
                // before this slot.  Since there are no occupied slots (and thus, no runs) in this
                // block up to `slot_block_index`, that means this slot is unused completely.
                // We indicate that by returning the index itself.
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
                //
                //NB POSSIBLE BUG: In the C code, this expression includes a ` - 1` and the end.  I
                //don't understand why.  `block_offset` is the distance to the end of the run; for
                //cases where the run is only 1 element long, it's 0.  We're returning the location
                //of the run end, not the location right before it, so why the " -1"?  In my tests
                //based on my logical undersatnding of the filter this produces incorrect results,
                //so I'm not using ' - 1' though I fear I'm just patching around some other
                //misunderstanding.
                return block::SLOTS_PER_BLOCK * block_index + block_offset;
            }
        }

        //Now we know there is at least one run in this block (because the `occupieds` rank for
        //this slot is non-zero), we need to figure out where the runends are and thereby determine
        //if this slot is part of such a run or not.
        //
        //To avoid having to jump around too many places, use the `offset` hint at the start of
        //this block, which tells us where the runend is for the first slot in that block.
        //On the off chance the slot we're looking for IS the first slot in the block that's easy
        //but even if not we can start the search there.
        //
        //Thanks to `block_offset` we know how many slots from slot 0 in this block is the
        //runend for the run which includes slot 0.  That's an optimization that let's us skip
        //ahead to where we're likely to find the runend we're interested in.  The concept here is
        //a bit heady.
        //
        //We know that whereever our runend is will be indicated by a bit set in the `runends`
        //bitmap corresponding to that slot containing the runend.  We know that slot 0 in this
        //block has a runend at `block_offset` slots ahead of itself (note this can and
        //often is zero, but the solution generalizes).  We also know from the structure invariants
        //that the runend for our slot `index` will not be before the runend for the first slot in
        //our block.
        //
        //Thus, we compute which block (possibly this one or a subsequent one) contains the runend
        //for slot 0 of this block, and mask out any runend bits between here and there.  Having
        //masked them out, we then scan forward from slot 0's runend, looking for the runend that
        //corresponds to the rank of our slot's bit in the `occupieds` bitmap.  If you don't
        //understand that relationship between `occupieds` and `runends` re-read the RSDF paper.
        //
        //Something that confused me was why do we set `runend_rank` to `intrablock_rank - 1` and not
        //`intrablock_rank`.  After all, the paper clearly shows that we use `RANK` to find the rank
        //of the slot in the `occupieds` bitmap and then `SELECT` to find the `RANK`-th bit in the
        //`runends` bitmap.  What the paper doesn't make so clear is that `SELECT`'s parameter
        //isn't a rank, it's an ordinal.  `SELECT(0)` selects the first set bit (just like the
        //first element in an array is at index 0).  So if we have a bit with rank 1, and we want
        //to use `SELECT` to find the 1st set bit, we do `SELECT(0)` not `SELECT(1)`.
        let mut runend_rank = intrablock_rank - 1;
        let mut current_block_index = block_index + (block_offset / block::SLOTS_PER_BLOCK);
        let mut block = self.get_block(current_block_index);

        //skipping all bits which are part of the slot 0 run since we know we come after those.
        //it's possible there are some set bits there which correspond to the ends of some earlier
        //run, which will confuse this computation.  we know our runend is at or after slot 0's
        //runend so we start looking from there.
        let mut skip_bits = block_offset % block::SLOTS_PER_BLOCK;

        loop {
            // scan for the run end AFTER the block slot 0 runend, up to the rank we computed for
            // the occupieds bit.  This corresponds to step 2 in figure 2 of the RSDF paper.
            match block.scan_for_runend(skip_bits, runend_rank) {
                block::ScanForRunendResult::FoundIt(runend_index) => {
                    //Found the block we were looking for at the specified index!
                    //This expressoin corresponds to step 3 of figure 2 of the RSDF paper
                    let runend = current_block_index * block::SLOTS_PER_BLOCK + runend_index;

                    //What we've found here is the runend that corresponds to the `occupieds` bit
                    //at or slightly before `index`.  This doesn't tell us that `index` is actually
                    //part of any run.  If `index` is part of this run we will know by comparing
                    //its position with the run end.  If it's not part of a run, we indicate that
                    //in this method by returning the index itself.
                    if runend >= index {
                        //Yup, it's part of a run
                        return runend;
                    } else {
                        //This index is after that run ends
                        return index;
                    }
                }
                block::ScanForRunendResult::KeepScanning(skipped_runends) => {
                    //The runend of the desired rank was not found.  It means the runend we're
                    //looking for must be in the next block.  Reduce the rank we're looking for
                    //based on the number we've skipped, and loop again
                    current_block_index += 1;
                    skip_bits = 0;
                    runend_rank -= skipped_runends;

                    //If there's an invalid structure such that there are more `occupieds` than
                    //`runends` or some `runends` come before their corresponding `occupieds` this
                    //will run past the end of the list here.  That always indicates a bug
                    //somewhere; it means the invariants have been violated
                    debug_assert!(current_block_index < self.blocks.len(),
                        "fatal integrity error: looking for run end of index {} scanned past the end of the blocks structure and did not find run end", 
                        index);

                    block = self.get_block(current_block_index);
                }
            }
        }
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

        // Quotient is occupied, so find its runend
        Some(self.find_run_end(quotient as usize))
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
                intrablock_index = block::SLOTS_PER_BLOCK - 1;
            }

            //Stop if we've reached another run end, otherwise keep scanning
            if block.is_runend(intrablock_index) {
                return None;
            }
        }
    }

    /// Given the index of a slot `index`, tests if that slot is empty.  Note that "empty" does not
    /// just mean "no quotient with home slot `index` has been inserted in the filter", it means
    /// "slot `index` is not part of any run for any quotient".  In other words it means the slot
    /// can be used to store a value.
    #[inline]
    pub fn is_slot_empty(&self, index: usize) -> bool {
        self.get_slot_runlength_lower_bound(index) == 0
    }

    /// Scans the structure starting at `index`, returning the index of the first slot that is
    /// empty (in the RSDF the term "unused" means the same thing).  That is, the slot is not part
    /// of any run and is available to store a remainder.
    ///
    /// # Returns
    ///
    /// The `cqf.c` method `find_first_empty_slot` assumes it can always find a free slot, because
    /// higher level code at insert time detects a high load factor and either resizes the
    /// structure or fails the insert.  I don't like that kind spooky action at a distance so I've
    /// changed this approach slightly.  As long as there is at least one available slot `free` at or
    /// after `index`, returns `Some(free)`.  If the structure is completely full returns `None`.
    #[inline]
    pub fn find_first_empty_slot(&self, index: usize) -> Option<usize> {
        let mut from = index;
        let mut t: usize;

        loop {
            t = self.get_slot_runlength_lower_bound(from);
            if t == 0 {
                break;
            }

            from += t;

            if from >= self.len() {
                //Scanned past the end of the structure and found no free slots
                return None;
            }
        }

        return Some(from + t);
    }

    /// Shifts the remainder values stored in slots ahead by one, from `start_index` to
    /// `start_index+slot_count-1`, and inserts `insert_value` into the now-unused slot at `start_index`.
    /// The contents of the slot at `start_index+slot_count-1` will be overwritten by this
    /// operation.  It's intended that this only be performed when that slot is known to be unused
    /// but this method does not enforce that.
    ///
    /// # Arguments
    ///
    /// * `start_index` - Index of the slot to start the shift from.  After the shift is complete,
    /// the value at `start_index` will now be at `start_index+1`, the value at `start_index+1`
    /// will now be at `start_index+2`, etc, on up to `start_index+slot_count-1`.
    ///
    /// * `slot_count` - Number of slots to include in the shift.  Must be at least 2 as a 1 slot
    /// shift makes no sense.  All slots from `start_index` to `start_index+slot_count-1`, inclusive, will be shifted ahead one slot.
    /// It's assumed, but not enforced, that `slot_count` corresponds to a slot that is not in use.
    #[inline]
    pub fn shift_slots_left(
        &mut self,
        start_index: usize,
        slot_count: usize,
        insert_val: u64,
    ) -> () {
        assert!(slot_count > 1);
        assert!(start_index + slot_count <= self.len());

        //Perform this operation one block at a time
        //Shifting spans blocks
        //Each block implements the shift-left-and-insert operation, and if passed a slot count
        //that is beyond the range of the block it returns the number of slots left to process
        //after that block.  It also returns the value of the slot pushed off the end of the block,
        //which we can feed back in to the next block.
        let mut shifted_slot = insert_val;

        for (block_index, start_intrablock_index, slot_length) in
            self.get_slot_range_iter(start_index, slot_count)
        {
            //NOTE: Rust as of this writing doesn't support tuple destructuring on assignment hence
            //this awkward two-step
            let rbits = self.rbits;
            let block = self.get_block_mut(block_index);
            let result: (usize, u64) =
                block.shift_slots_left(rbits, start_intrablock_index, slot_length, shifted_slot);

            shifted_slot = result.1;
        }
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

    /// Given the index of a slot, attempts to compute the length of the run starting at `index`.
    /// I a write "attempts" because this avoids any complex computations and computes a lower
    /// bound on the length of the run.  The run might be longer than this method computes, but it
    /// will never be shorter.
    ///
    /// # Returns
    ///
    /// The remaining length of the run of which `index` is one element.  This length
    /// includes `index`, so if there is a run with a single element at `index`,
    /// this method returns `1`.  If `index` is unused, returns `0`.
    ///
    /// # Remarks
    ///
    /// This corresponds to the `cqf.c` function `offset_lower_bound`.
    #[inline]
    fn get_slot_runlength_lower_bound(&self, index: usize) -> usize {
        let (slot_block_index, slot_intrablock_index) = self.get_slot_location(index);
        self.blocks[slot_block_index].get_slot_runlength_lower_bound(slot_intrablock_index)
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

    /// Gets a mutable reference to a block.  Using Rust lifetime trickery ensures this doesn't
    /// result in a dangling pointer.
    #[inline]
    fn get_block_mut<'s: 'block, 'block>(&'s mut self, index: usize) -> &'block mut block::Block {
        debug_assert!(
            index < self.blocks.len(),
            "block index {} is out of bounds",
            index
        );

        &mut self.blocks[index]
    }

    /// Does the repetitive and simple math to translate from a slot index to a block index and the
    /// index within that block where the slot is located.
    #[inline]
    fn get_slot_location(&self, index: usize) -> (usize, usize) {
        let block_index = index / block::SLOTS_PER_BLOCK;
        let intrablock_index = index % block::SLOTS_PER_BLOCK;

        (block_index, intrablock_index)
    }

    /// When operating on a range of slots, its helpful to know how that range of slots is
    /// distributed across the blocks.  Often times we can perform an operation more efficiently if
    /// we know the entire block is included.  A good example of this is shifting, which has an
    /// optimized pathway for slot ranges that span an entire block.
    ///
    /// # Returns
    ///
    /// A tuple:
    ///
    /// `(start_block_index, start_intrablock_index, end_block_index, end_intrablock_index)`
    #[inline]
    fn get_slot_range_location(
        &self,
        start_index: usize,
        slot_length: usize,
    ) -> (usize, usize, usize, usize) {
        let (start_block_index, start_intrablock_index) = self.get_slot_location(start_index);
        let (end_block_index, end_intrablock_index) =
            self.get_slot_location(start_index + slot_length - 1);

        (
            start_block_index,
            start_intrablock_index,
            end_block_index,
            end_intrablock_index,
        )
    }

    #[inline]
    fn get_slot_range_iter(&self, start_index: usize, slot_length: usize) -> SlotRangeIterator {
        let (start_block_index, start_intrablock_index, end_block_index, end_intrablock_index) =
            self.get_slot_range_location(start_index, slot_length);

        SlotRangeIterator::new(
            start_block_index,
            start_intrablock_index,
            end_block_index,
            end_intrablock_index,
        )
    }
}

#[cfg(test)]
mod physicaldata_tests {
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
    pub fn find_run_end_tests() {
        let mut pd = PhysicalData::new(TEST_SLOTS, TEST_RBITS);

        //find_run_end has some complicated logic which requires some complicated set up in order
        //to test.  Buckle up.

        //Structure is empty, every slot should be its own runend as none of them are actually part
        //of runs.
        for index in 0..TEST_SLOTS {
            assert_eq!(index, pd.find_run_end(index));
        }

        //Single slot is occupied and its own runend.  All other slots unused.
        const TEST_SLOT: usize = 100;
        pd.set_occupied(TEST_SLOT, true);
        pd.set_runend(TEST_SLOT, true);
        for index in 0..TEST_SLOTS {
            assert_eq!(index, pd.find_run_end(index), "index {}", index);
        }

        //Single slot occupied, run is longer than one slot
        pd.set_runend(TEST_SLOT, false);
        pd.set_runend(TEST_SLOT + 5, true);
        for index in 0..TEST_SLOTS {
            if index >= TEST_SLOT && index <= TEST_SLOT + 5 {
                assert_eq!(TEST_SLOT + 5, pd.find_run_end(index), "index {}", index);
            } else {
                assert_eq!(index, pd.find_run_end(index), "index {}", index);
            }
        }

        //Single run spans a block
        //This is complex enough let's start with a fresh structure
        let mut pd = PhysicalData::new(TEST_SLOTS, TEST_RBITS);

        const RUN_LENGTH: usize = 1000; //this spans multiple blocks and overflows the u8 offset field
        const RUN_END: usize = TEST_SLOT + RUN_LENGTH - 1;
        pd.set_occupied(TEST_SLOT, true);
        pd.set_runend(RUN_END, true);
        let mut block_index = (TEST_SLOT + block::SLOTS_PER_BLOCK - 1) / block::SLOTS_PER_BLOCK;
        loop {
            let block_offset = RUN_END - block_index * block::SLOTS_PER_BLOCK;
            pd.blocks[block_index].set_offset(block_offset);
            println!("block {} has offset {}", block_index, block_offset);
            block_index += 1;
            if block_index > RUN_END / block::SLOTS_PER_BLOCK {
                break;
            }
        }

        for index in 0..TEST_SLOTS {
            if index >= TEST_SLOT && index <= RUN_END {
                assert_eq!(RUN_END, pd.find_run_end(index), "index {}", index);
            } else {
                assert_eq!(index, pd.find_run_end(index), "index {}", index);
            }
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
    pub fn find_first_empty_slot_tests() {
        let mut pd = PhysicalData::new(TEST_SLOTS, TEST_RBITS);

        //Initially all slots are empty, so the scan should stop at the index
        for index in 0..TEST_SLOTS {
            assert_eq!(Some(index), pd.find_first_empty_slot(index));
        }

        //Make some slots used
        const TEST_SLOT: usize = 100;
        const RUN_LENGTH: usize = 5;
        for index in 0..RUN_LENGTH {
            pd.set_occupied(TEST_SLOT + index, true);
            pd.set_runend(TEST_SLOT + index, true);
        }

        for index in 0..TEST_SLOTS {
            if index >= TEST_SLOT && index <= TEST_SLOT + RUN_LENGTH {
                assert_eq!(
                    Some(TEST_SLOT + RUN_LENGTH),
                    pd.find_first_empty_slot(index)
                );
            } else {
                assert_eq!(Some(index), pd.find_first_empty_slot(index));
            }
        }

        //Pathological case (also very unlikely with a real-world RSQF structure)
        //all slots used
        for index in 0..TEST_SLOTS {
            pd.set_occupied(index, true);
            pd.set_runend(index, true);
        }

        //In this pathological case, find_first_empty_slot operates one slot at a time until the
        //end.  It's too slow to run this for all of the test slots.  Run it for slot 0, which
        //means it will scan the entire structure.  If it doesn't find any empty slots on that
        //pass, it definitely won't on passes for higher slot indexes
        assert_eq!(None, pd.find_first_empty_slot(0));
    }

    #[test]
    pub fn shift_left_test() {
        // Shifting is a complicated process, though the `bitarray` and `block` modules
        // abstract most of the details away.  This test focuses on exercising the inter-block
        // shifting mechanics.
        //
        // Start with a PhysicalData instance that has known values in each slot.  Perform some
        // shifts and confirm the results are as expected.
        fn get_slot_test_value(i: usize) -> u64 {
            (i as u64) & bitmask!(block::BITS_PER_SLOT)
        };

        fn get_runend_test_value(i: usize) -> bool {
            if i % 10 == 0 {
                true
            } else {
                false
            }
        }

        fn shift_left_slow(
            pd: &mut PhysicalData,
            start_index: usize,
            slot_length: usize,
            insert_val: u64,
            insert_bit: bool,
        ) -> () {
            let mut shifted_val = insert_val;
            let mut shifted_bit = insert_bit;

            for i in start_index..start_index + slot_length {
                shifted_val = pd.set_slot(i, shifted_val);
                let old_runend = pd.is_runend(i);
                pd.set_runend(i, shifted_bit);
                shifted_bit = old_runend;
            }
        }

        let mut pd = PhysicalData::new(TEST_SLOTS, TEST_RBITS);
        for index in 0..TEST_SLOTS {
            pd.set_slot(index, get_slot_test_value(index));
            pd.set_runend(index, get_runend_test_value(index));
        }

        //pd is no longer mutable it's a reference
        let pd = pd;

        // each of these test cases describes a shift operation which we will perform using
        // shift_slots_left/shift_runends_left and again manually using get_slot/set_slot/get_runend/set_runend, and compare the resutls
        //
        // ( start_index, slot_length, insert_value, insert_bit)
        let test_cases = [
            //The entire structure
            (0, TEST_SLOTS, 0x1ff, true),
            //A few slots starting on a block boundary
            (block::SLOTS_PER_BLOCK, 5, 0x1ff, true),
            //A few slots ending on a block boundary
            (block::SLOTS_PER_BLOCK - 5, 5, 0x1ff, true),
            //A few slots, straddling a block boundary
            (block::SLOTS_PER_BLOCK - 5, 10, 0x1ff, true),
            //The entire contents of just one block
            (
                block::SLOTS_PER_BLOCK * 2,
                block::SLOTS_PER_BLOCK,
                0x1ff,
                true,
            ),
            //The entire contents of multiple blocks
            (
                block::SLOTS_PER_BLOCK * 2,
                block::SLOTS_PER_BLOCK * 3,
                0x1ff,
                true,
            ),
        ];

        for (start_index, slot_length, insert_value, insert_bit) in &test_cases {
            let context =
                format!(
                "TEST_SLOTS={} SLOTS_PER_BLOCK={} start_index={} slot_length={} insert_value={} insert_bit={}",
                TEST_SLOTS, block::SLOTS_PER_BLOCK, start_index, slot_length, insert_value, insert_bit
            );

            println!("shift_slots_left test case {}", context);

            let mut expected_pd = pd.clone();
            let mut actual_pd = pd.clone();

            shift_left_slow(
                &mut expected_pd,
                *start_index,
                *slot_length,
                *insert_value,
                *insert_bit,
            );
            actual_pd.shift_slots_left(*start_index, *slot_length, *insert_value);
            actual_pd.shift_runends_left(*start_index, *slot_length, *insert_bit);

            assert_data_eq(&expected_pd, &actual_pd, &context);
        }
    }

    #[test]
    pub fn get_slot_runlength_lower_bound_test() {
        let mut pd = PhysicalData::new(TEST_SLOTS, TEST_RBITS);

        //Initially all slots are empty and all have 0 run length lower bound
        for index in 0..TEST_SLOTS {
            assert_eq!(0, pd.get_slot_runlength_lower_bound(index));
        }

        //If the slot is occupied and its runend is in the same slot (run length of 1 element) the
        //run length lower bound should be 1, and all the other slots 0
        const TEST_SLOT: usize = 100;
        pd.set_occupied(TEST_SLOT, true);
        pd.set_runend(TEST_SLOT, true);

        for index in 0..TEST_SLOTS {
            if index != TEST_SLOT {
                assert_eq!(0, pd.get_slot_runlength_lower_bound(index));
            } else {
                assert_eq!(1, pd.get_slot_runlength_lower_bound(index));
            }
        }

        //If all slots contain single element runs they should all have run length lower bound 1
        for index in 0..TEST_SLOTS {
            pd.set_occupied(index, true);
            pd.set_runend(index, true);
        }

        for index in 0..TEST_SLOTS {
            assert_eq!(1, pd.get_slot_runlength_lower_bound(index));
        }

        //start again with a fresh structure
        let mut pd = PhysicalData::new(TEST_SLOTS, TEST_RBITS);

        //Here's a run that starts at the beginning of a block, and finishes at the end of the
        //block.
        const BLOCK_INDEX: usize = TEST_SLOT / block::SLOTS_PER_BLOCK;
        pd.set_occupied(BLOCK_INDEX * block::SLOTS_PER_BLOCK, true);
        pd.set_runend(
            BLOCK_INDEX * block::SLOTS_PER_BLOCK + block::SLOTS_PER_BLOCK - 1,
            true,
        );
        pd.blocks[BLOCK_INDEX].set_offset(block::SLOTS_PER_BLOCK - 1);
        for index in 0..block::SLOTS_PER_BLOCK {
            assert_eq!(
                block::SLOTS_PER_BLOCK - index,
                pd.get_slot_runlength_lower_bound(BLOCK_INDEX * block::SLOTS_PER_BLOCK + index)
            );
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

    fn assert_data_eq(expected: &PhysicalData, actual: &PhysicalData, context: &str) {
        assert_eq!(expected.len(), actual.len(), "context: {}", context);

        for i in 0..expected.len() {
            assert_eq!(
                expected.get_slot(i),
                actual.get_slot(i),
                "slot={} context: {}",
                i,
                context,
            );
            assert_eq!(
                expected.is_runend(i),
                actual.is_runend(i),
                "runend slot={} context: {}",
                i,
                context,
            );
        }
    }
}

impl SlotRangeIterator {
    fn new(
        start_block_index: usize,
        start_intrablock_index: usize,
        end_block_index: usize,
        end_intrablock_index: usize,
    ) -> SlotRangeIterator {
        debug_assert!(start_intrablock_index < block::SLOTS_PER_BLOCK);
        debug_assert!(end_intrablock_index < block::SLOTS_PER_BLOCK);
        SlotRangeIterator {
            start_block_index,
            start_intrablock_index,
            end_block_index,
            end_intrablock_index,
            next_block_index: Some(start_block_index),
        }
    }
}
/// Implements iteration over a range of slots, where each item yielded is a tuple describing a
/// block containing some part of that range:
///
/// `(block_index, start_intrablock_index, slot_length)`
impl Iterator for SlotRangeIterator {
    type Item = (usize, usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        //If next_block_index is None then return None, otherwise do something with the actual
        //index value
        self.next_block_index.map(|next_block| {
            if self.start_block_index == self.end_block_index {
                //There's only one block; this will be easy
                self.next_block_index = None;
                (
                    self.start_block_index,
                    self.start_intrablock_index,
                    self.end_intrablock_index - self.start_intrablock_index + 1,
                )
            } else {
                if next_block < self.end_block_index {
                    //There are more blocks
                    self.next_block_index = Some(next_block + 1);
                } else {
                    //That's the last one
                    self.next_block_index = None;
                }

                //Multiple blocks.  Need to figure out where the next block is and go with that.
                if next_block == self.start_block_index {
                    //Current block is the start block, but it's there's more than one block so we
                    //know this block goes from the start_intrablock_index to the end of the block
                    (
                        self.start_block_index,
                        self.start_intrablock_index,
                        block::SLOTS_PER_BLOCK - self.start_intrablock_index,
                    )
                } else if next_block == self.end_block_index {
                    //This is the last block
                    //We know this isn't the only block, so the range starts at offset 0 and runs
                    //to the end block index (+1 here because we're not yielding the index we're
                    //yielding the length
                    (self.end_block_index, 0, self.end_intrablock_index + 1)
                } else {
                    //This is some block in between start and end.  Since it's not the start and
                    //it's not the end we know it is an entire block
                    (next_block, 0, block::SLOTS_PER_BLOCK)
                }
            }
        })
    }
}

#[cfg(test)]
mod slot_range_iterator_tests {
    use super::*;

    #[test]
    pub fn iterator_tests() {
        // test cases:
        //
        // (start_block_index, start_intrablock_index, end_block_index, end_intrablock_index,
        // expected_blocks)
        let test_data = [
            //Start with an easy single slot range; start and end index are the same slot
            (0, 0, 0, 0, vec![(0, 0, 1)]),
            //A range of two slots, same block
            (0, 0, 0, 1, vec![(0, 0, 2)]),
            //An entire block
            (
                1,
                0,
                1,
                block::SLOTS_PER_BLOCK - 1,
                vec![(1, 0, block::SLOTS_PER_BLOCK)],
            ),
            //Multiple entire blocks
            (
                1,
                0,
                3,
                block::SLOTS_PER_BLOCK - 1,
                vec![
                    (1, 0, block::SLOTS_PER_BLOCK),
                    (2, 0, block::SLOTS_PER_BLOCK),
                    (3, 0, block::SLOTS_PER_BLOCK),
                ],
            ),
            //Parts of the start and end block, and whole blocks in between
            (
                1,
                5,
                4,
                block::SLOTS_PER_BLOCK - 32,
                vec![
                    (1, 5, block::SLOTS_PER_BLOCK - 5),
                    (2, 0, block::SLOTS_PER_BLOCK),
                    (3, 0, block::SLOTS_PER_BLOCK),
                    (4, 0, block::SLOTS_PER_BLOCK - 32 + 1),
                ],
            ),
        ];

        for (
            start_block_index,
            start_intrablock_index,
            end_block_index,
            end_intrablock_index,
            expected_blocks,
        ) in &test_data
        {
            let mut range = SlotRangeIterator::new(
                *start_block_index,
                *start_intrablock_index,
                *end_block_index,
                *end_intrablock_index,
            );

            let blocks = range.collect::<Vec<_>>();
            assert_eq!(*expected_blocks, blocks, "range: {:?}", range);
        }
    }

}
