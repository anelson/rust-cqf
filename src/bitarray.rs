//! Describes a `BitArray` trait and purpose-built implementations for both `u64` arrays and single
//! `u64` types.
use bitfiddling::BitFiddling;
use std::mem;

/// An array where each element is a single bit.  The RSQF structure uses `u64` values to represent
/// arrays of 64 single bit values, and takes advantage of some binary trickery to do various
/// computations at high speed.  To the extent those operations are not RSQF specific but are
/// general purpose bit-flippery, they are abstracted away behind this trait and its corresponding
/// implementation on `u64`
pub trait SingleBitArray {
    #[inline]
    fn get_bit(self, index: usize) -> bool;

    #[inline]
    fn set_bit(&mut self, index: usize, val: bool) -> ();

    /// Returns the bit position of the `n`th set bit, or `None` if there is no such bit.
    ///
    /// This is often called a SELECT operation.
    #[inline]
    fn find_nth_set_bit(self, n: usize) -> Option<usize>;

    /// Masks off the first `n` bits and returns them in a `u64`.  Obviously this assumes `n` is
    /// less than 64.
    #[inline]
    fn get_first_n_bits(self, n: usize) -> u64;

    /// Counts the number of set bits in the array.  This is often called a POPCNT (for "population
    /// count") operation.
    #[inline]
    fn count_set_bits(self) -> usize;

    /// Counts how many set bits are in the array from bit 0 counting `n` bits.  This is also known
    /// as a RANK operation
    #[inline]
    fn count_first_n_set_bits(self, n: usize) -> usize;
}

impl SingleBitArray for u64 {
    #[inline]
    fn get_bit(self, index: usize) -> bool {
        self & (1u64 << index) != 0
    }

    #[inline]
    fn set_bit(&mut self, index: usize, val: bool) -> () {
        let mask = 1u64 << index;
        let bitvalue = if val { mask } else { 0u64 };

        *self = (*self & !mask) | bitvalue;
    }

    /// Returns the bit position of the `n`th set bit, or `None` if there is no such bit.
    ///
    /// This is often called a SELECT operation.
    #[inline]
    fn find_nth_set_bit(self, n: usize) -> Option<usize> {
        self.bitselect(n)
    }

    /// Masks off the first `n` bits and returns them in a `u64`.  Obviously this assumes `n` is
    /// less than 64.
    #[inline]
    fn get_first_n_bits(self, n: usize) -> u64 {
        self & bitmask!(n)
    }

    /// Counts the number of set bits in the array.  This is often called a POPCNT (for "population
    /// count") operation.
    #[inline]
    fn count_set_bits(self) -> usize {
        self.popcnt()
    }

    /// Counts how many set bits are in the array from bit 0 counting `n` bits.  This is also known
    /// as a RANK operation
    #[inline]
    fn count_first_n_set_bits(self, n: usize) -> usize {
        self.popcnt_first_n(n)
    }
}

/// A multi-bit array stores unsigned integer values of some fixed bit length `n`, packed neatly
/// together and stored in a `u64` array underneath.  This is useful because the RSQF structure
/// maintains an array of what it calls 'slots', each of which is `r` bits long where `r` is the
/// number of bits in the remainder.  Typically `r` would be on the order of 7, up to maybe 13 or
/// 14, but in theory it can be from 2 up to 63 (though at both extremes the structure becomes
/// practically useless).
pub trait MultiBitArray {
    #[inline]
    fn get_total_slots(&self, nbits: usize) -> usize;

    /// Gets the `nbits`-bit slot at `index` (`index` is the slot index, not the bit index)
    #[inline]
    fn get_slot(&self, nbits: usize, index: usize) -> u64;

    #[inline]
    /// Sets the `nbits`-bit slot at `index` (`index` is the slot index, not the bit index)
    ///
    /// # Returns
    ///
    /// Returns the previous value at index `index`.
    fn set_slot(&mut self, nbits: usize, index: usize, val: u64) -> u64;

    /// Shifts some or all of the slots right by one slot.  This obviously could shift one slot off
    /// the end of the array.
    ///
    /// # Arguments
    ///
    /// `nbits` - The size of each slot in bits
    /// `start_index` - The index of the slot in this block where the shift operation
    /// should start.  `0` starts the shift from the start of the block.
    /// `slot_count` - The number of slots to shift.  Note that this can be more than the number of
    /// slots in a block; if so, `shift_slots` will shift from `start_index` to the end of
    /// the block, and include in its return value the `slot_count` minus the number of slots it
    /// shifted already; this lets the caller call this method in a loop iteratively shifting all
    /// of the slots.
    /// `insert_value` - If a previous block also shifted its slots, this is the value of
    /// the slot shifted off the end of that block, which will be placed into this block at
    /// `start_index` after its contents are shifted up one.  Set to `0` for the block
    /// where the shifting operation starts.
    ///
    /// # Returns
    ///
    /// Returns a tuple which makes it easy to perform the shift iteratively.
    ///
    /// `(remaining_slot_count, top_slot)`:
    ///
    /// * `remaining_slot_count` - How many slots are still to be shifted, after this block's shift
    /// operation has completed.  A convenience value; it's `slot_count` minus the number of slots
    /// effected by this shift operation.
    /// * `top_slot` - The value in the highest-most slot in this shift region, which due to the shift has
    /// overflowed the region and should be placed into the next array if the shift operation is
    /// going to continue.  This can be pased back in to `shift_slots_left` as the
    /// `insert_value` argument to ensure it's placed in the next array in the right
    /// place.
    #[inline]
    fn shift_slots_left(
        &mut self,
        nbits: usize,
        start_index: usize,
        slot_count: usize,
        insert_value: u64,
    ) -> (usize, u64);

    /// Inserts the `nbits`-bit slot at `index` (`index` is the slot index, not the bit index)
    ///
    /// "insert" is different from "set" in that "set" overwrites whatever slot is at `index`,
    /// while "insert" shifts right any existing slots by `nbits`, to make room for a new
    /// slot at `index`.  Because `MultiBitArray` is of a fixed size, this shift means the
    /// slot at the very end of the array is shifted off of the array.
    ///
    /// # Returns
    ///
    /// Returns the `nbits`-bit value shifted off the end of the array as a result of the shift.
    #[inline]
    fn insert_slot(&mut self, nbits: usize, index: usize, val: u64) -> Option<u64>;

    /// Deletes the `nbits`-bit slot at `index` (`index` is the slot index, not the bit index)
    ///
    /// "delete" here is the inverse of "insert"; subsequent slots will be shifted right.
    #[inline]
    fn delete_slot(&mut self, nbits: usize, index: usize) -> ();
}

/// Specialized implementation of MultiBitArray for slices of `u64`.  We expect this to actually be
/// a fixed-length array of `u64` but it's more convenient to specify in terms of a slice.  If this
/// introduces an excessive performance penalty then we'll have to use macros and generate
/// implementations for various possible sizes of `u64` arrays.  
impl MultiBitArray for [u64] {
    fn get_total_slots(&self, nbits: usize) -> usize {
        self.len() * 64 / nbits
    }

    #[inline]
    fn get_slot(&self, nbits: usize, index: usize) -> u64 {
        let (slot_word_index, slot_bit_offset_in_word, bits_in_first_word, bits_in_second_word) =
            find_slot(self, nbits, index);
        let nbits = nbits as u64;

        //Each array contains some number of slots where each slot is nbits bits long.  The slots
        //are stored in an array of u64 words.  If nbits is an integer divisor of the number of
        //slots
        //then slots cannot span multiple words, but nbits is a runtime parameter so that can't be
        //assumed.
        //
        //Therefore when attempting to read a slot value there are two possible cases:
        //* slot is entirely within a single word
        //* slot starts on one word and ends on another
        if bits_in_second_word == 0 {
            //All of the slot is in one word; this statistically is the more common case
            (self[slot_word_index as usize] >> slot_bit_offset_in_word) & bitmask!(nbits)
        } else {
            //This slot starts on one word and ends on the following word so extract both pieces
            //then put them together
            //If a slot spans two words, we know the the part of the slot's value in the second
            //word starts at bit 0 in that word, so unlike for the first part which requires a bit
            //shift and then a bit mask, the second part can be obtained with just a bit mask
            let first_part = (self[slot_word_index as usize] >> slot_bit_offset_in_word)
                & bitmask!(bits_in_first_word);
            let second_part = self[slot_word_index as usize + 1] & bitmask!(bits_in_second_word);

            first_part | (second_part << bits_in_first_word)
        }
    }

    #[inline]
    fn set_slot(&mut self, nbits: usize, index: usize, val: u64) -> u64 {
        let (slot_word_index, slot_bit_offset_in_word, bits_in_first_word, bits_in_second_word) =
            find_slot(self, nbits, index);

        if bits_in_second_word == 0 {
            //This slot is entirely contained within one word
            let old_word = self[slot_word_index as usize];
            let slot_bitmask = bitmask!(nbits) << slot_bit_offset_in_word;
            let new_word =
                (old_word & !slot_bitmask) | ((val << slot_bit_offset_in_word) & slot_bitmask);

            self[slot_word_index as usize] = new_word;

            //Return the previous value of this slot
            ((old_word & slot_bitmask) >> slot_bit_offset_in_word)
        } else {
            //This slot starts on one word and ends on the following word so extract both pieces
            //and then put them in the two words

            //If a slot spans two words, we know the the part of the slot's value in the second
            //word starts at bit 0 in that word, so unlike for the first part which requires a bit
            //shift and then a bit mask, the second part can be obtained with just a bit mask
            let old_word = self[slot_word_index as usize];
            let slot_bitmask = bitmask!(bits_in_first_word) << slot_bit_offset_in_word;
            let first_part = (old_word & slot_bitmask) >> slot_bit_offset_in_word;
            let new_word =
                (old_word & !slot_bitmask) | ((val << slot_bit_offset_in_word) & slot_bitmask);

            self[slot_word_index as usize] = new_word;

            let old_word = self[slot_word_index as usize + 1];
            let slot_bitmask = bitmask!(bits_in_second_word);
            let second_part = old_word & slot_bitmask;
            let new_word =
                (old_word & !slot_bitmask) | ((val >> bits_in_first_word) & slot_bitmask);

            self[slot_word_index as usize + 1] = new_word;

            // Finally, return the previous value
            first_part | (second_part << bits_in_first_word)
        }
    }

    #[inline]
    fn shift_slots_left(
        &mut self,
        nbits: usize,
        start_index: usize,
        slot_count: usize,
        insert_value: u64,
    ) -> (usize, u64) {
        debug_assert!(start_index < nbits);
        debug_assert!(slot_count > 0);
        debug_assert!((insert_value & !bitmask!(nbits)) == 0);
        /*
         *        let mut shifted_slot: u64;
         *
         *        //Depdning upon the start_index and slot_count, this shift operation can include
         *        //one or more of the following variations:
         *        //
         *        /[> shift bits within a single word, starting from the middle of the word up to the end
         *        /[> shift bits within a single word, starting from 0 and up to a place in the middle of the
         *        //word
         *        /[> shift bits within a single word entirely within the middle of the word
         *        /[> shift bits across the entirety of a whole word
         *
         *        //Figure out where the first and last slot to be shifted are.
         *        let (
         *            start_word_index,
         *            start_bit_offset_in_word,
         *            start_bits_in_first_word,
         *            start_bits_in_second_word,
         *        ) = find_slot(self, nbits, start_index);
         *
         *        //Might need to clamp the end slot if slot_count exceeds the slots in this array
         *        let end_index = if start_index + slot_count > self.get_total_slots(nbits) {
         *            self.get_total_slots(nbits) - 1
         *        } else {
         *            start_index + slot_count - 1
         *        };
         *
         *        let (
         *            end_word_index,
         *            end_bit_offset_in_word,
         *            end_bits_in_first_word,
         *            end_bits_in_second_word,
         *        ) = find_slot(self, nbits, end_index);
         *
         *        if start_index == 0 && slot_count >= nbits {
         *            //This is the most efficient path.  The entire contents of this block will be shifted
         *            //by `nbits` bits.  Work through the words from end to start
         *            let mut shifted_slot: u64 = insert_value;
         *
         *            for word_index in 0..BITS_PER_SLOT {
         *                let word = self.slots[word_index];
         *                let new_word = word << nbits | shifted_slot;
         *                shifted_slot = word >> (64 - nbits);
         *
         *                self.slots[word_index] = new_word;
         *            }
         *
         *            // That's it; shifted the entire block left by nbits; shifted_slot contains the value
         *            // of the top-most slot in the block which should be shifted into the bottom of the
         *            // next block.
         *            (slot_count - nbits, shifted_slot)
         *        } else if start_index == 0 {
         *            //Starting at the beginning of the block but the end is also in this block.
         *            //Figure out how many entire words are covered, and if there's part of a word at the
         *            //end handle that separately;
         *            let whole_words = slot_count * BITS_PER_SLOT / 64;
         *
         *            let mut shifted_slot: u64 = insert_value;
         *
         *            for word_index in 0..whole_words {
         *                let word = self.slots[word_index];
         *                let new_word = word << nbits | shifted_slot;
         *                shifted_slot = word >> (64 - nbits);
         *
         *                self.slots[word_index] = new_word;
         *            }
         *
         *            //For any remaining slots after the whole words just do the shift a slot at a time.
         *            let remaining_bits = (slot_count * BITS_PER_SLOT) % 64;
         *            let final_word = whole_words;
         *
         *            let word = self.slots[final_word];
         *
         *            (0, 0) //TODO: This is just to get the code to compile it's not right
         *        } else {
         *            panic!("NYI");
         *        }
         */
        panic!("NYI");
    }

    #[inline]
    #[allow(unused_variables)]
    #[allow(dead_code)]
    fn insert_slot(&mut self, nbits: usize, index: usize, val: u64) -> Option<u64> {
        panic!("NYI");
    }

    #[inline]
    #[allow(unused_variables)]
    #[allow(dead_code)]
    fn delete_slot(&mut self, nbits: usize, index: usize) -> () {
        panic!("NYI");
    }
}

/// Given a relative index of a slot and the `nbits` number of bits per slot, finds the
/// location of the bits of this slot in this block.  Note that a slot can either be entirely
/// in one word, or spanning the boundary between two words.  This function returns a tuple
/// which represents the following information:
///
/// `(slot_word_index, slot_bit_offset_in_word, bits_in_first_word, bits_in_second_word)`
///
/// * `slot_word_index` - The 0-based index into the `slots` array where this slot's data
/// starts
/// * `slot_bit_offset_in_word` - The 0-based bit position where this slot's data starts
/// * `bits_in_first_word` - The number of bits located in the word at `slot_word_index`; this
/// is always at least 1
/// * `bits_in_second_word` - The number of bits of this slot's data located starting at bit 0
/// in the word at index `slot_word_index + 1`.  This may be 0 for slots located entirely in
/// one word
#[inline]
#[allow(dead_code)] // Just for now until higher level modules call this
fn find_slot(arr: &[u64], nbits: usize, index: usize) -> (usize, usize, usize, usize) {
    debug_assert!(nbits > 0);

    let total_slots = arr.get_total_slots(nbits);
    let block_word_size = mem::size_of::<u64>() * 8;
    assert!(index < total_slots);

    //Each block contains total_slots slots where each slot is nbits bits long.  The slots
    //are stored in an array of u64 words.  If nbits is an integer divisor of total_slots
    //then slots cannot span multiple words, but nbits is a runtime parameter so that can't be
    //assumed.
    //
    //Therefore when attempting to read a slot value there are two possible cases:
    //* slot is entirely within a single word
    //* slot starts on one word and ends on another
    let slot_word_index = index * nbits / block_word_size;
    let slot_bit_offset_in_word = index * nbits % block_word_size;

    if slot_bit_offset_in_word + nbits <= block_word_size {
        //This slot is entirely contained within one word
        (slot_word_index, slot_bit_offset_in_word, nbits, 0)
    } else {
        //This slot starts on one word and ends on the following word so extract both pieces
        //then put them together

        let bits_in_first_word = block_word_size - slot_bit_offset_in_word;
        let bits_in_second_word = nbits - bits_in_first_word;

        (
            slot_word_index,
            slot_bit_offset_in_word,
            bits_in_first_word,
            bits_in_second_word,
        )
    }
}

/// Similar to `find_slot`, but instead of looking for a specific slot it looks for a range of
/// slots and computes their start and end boundaries.  This is useful when performing shift
/// operations
///
/// Given a index of a slot and the `nbits` number of bits per slot and a `count` of slots, finds the
/// location of the start and end of the range of bits in the array.
///
/// # Returns
///
/// `(start_word_index, bits_in_start_word, end_word_index, bits_in_end_word)`
///
/// * `start_word_index` - The index of the word that contains the first bit of slot `index`
/// * `bits_in_start_word` - How many bits of `start_word_index` contain this range.  If the range
/// spans multiple words, this will be the upper bits of that word.  
/// * `end_word_index` - The index of the word that contains the last bit of slot `index+count-1`.
/// If the range does not span multiple words, this is the same as `start_word_index`.
/// * `bits_in_end_word` - The number of bits in `end_word_index` that contain this range.  If the
/// range spans multiple words, these will be the lower bits of that word.
#[inline]
#[allow(dead_code)] // Just for now until higher level modules call this
fn find_slot_range(
    arr: &[u64],
    nbits: usize,
    start_index: usize,
    count: usize,
) -> (usize, usize, usize, usize) {
    debug_assert!(nbits > 0);

    let total_slots = arr.get_total_slots(nbits);
    let block_word_size = mem::size_of::<u64>() * 8;
    assert!(start_index + count <= total_slots);

    //Figure out where the first and last slot to be shifted are.
    let end_index = start_index + count - 1;
    let (start_word_index, start_bit_offset_in_word, _, _) = find_slot(arr, nbits, start_index);

    let (end_word_index, end_bit_offset_in_word, end_bits_in_first_word, end_bits_in_second_word) =
        find_slot(arr, nbits, end_index);

    //how many bits, total, are in the start word?
    if start_word_index == end_word_index && end_bits_in_second_word == 0 {
        let total_bits = count * nbits;

        // in debug builds sanity-check this total; this expression and the one used for total_bits
        // should be equivalent
        debug_assert!(
            total_bits
                == end_bit_offset_in_word + end_bits_in_first_word - start_bit_offset_in_word
        );

        return (start_word_index, total_bits, end_word_index, total_bits);
    }

    //start and end words are not the same so this spans multiple words.
    let bits_in_start_word = block_word_size - start_bit_offset_in_word;

    //end_word_index is the index of the word where the last slot STARTS, not neceessarily where
    //all of its bits are.  If the `end_bits_in_second_word` value is non-zero, it means this last
    //slot spills over into a word AFTER `end_word_index`
    let (end_word_index, bits_in_end_word) = if end_bits_in_second_word == 0 {
        (
            end_word_index,
            end_bit_offset_in_word + end_bits_in_first_word,
        )
    } else {
        (end_word_index + 1, end_bits_in_second_word)
    };

    (
        start_word_index,
        bits_in_start_word,
        end_word_index,
        bits_in_end_word,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    extern crate rand;

    const BITS_PER_SLOT: usize = 9;
    const SLOTS_PER_TEST_BLOCK: usize = 64;
    const WORDS_PER_BLOCK: usize = (BITS_PER_SLOT * SLOTS_PER_TEST_BLOCK + 63) / 64;

    #[test]
    pub fn get_slot_empty_array_tests() {
        let block = [0u64; WORDS_PER_BLOCK];

        for i in 0..SLOTS_PER_TEST_BLOCK {
            println!("slot {}", i);
            assert_eq!(0, block.get_slot(BITS_PER_SLOT, i));
        }
    }

    #[test]
    pub fn set_slot_empty_block_tests() {
        let mut block = [0u64; WORDS_PER_BLOCK];

        block.set_slot(BITS_PER_SLOT, 0, 0x1ff);
        assert_eq!(0x1ff, block.get_slot(BITS_PER_SLOT, 0));
    }

    #[test]
    pub fn get_set_block_tests() {
        //generate an array of random u64 values, one for each possible slot in the block
        //iterate over them, calling get_slot and confirming 0, set_slot, then get_slot again
        //confirming the correct value
        //
        //After all values are obtained, run through again, this time calling get_slot to confirm
        //the expected value, then set_slot to set a 0 value, and get_slot to confirm the 0 value
        let mut random_values: Vec<u64> = Vec::with_capacity(SLOTS_PER_TEST_BLOCK);
        for _ in 0..SLOTS_PER_TEST_BLOCK {
            random_values.push(rand::random::<u64>());
        }

        let mut block = [0u64; WORDS_PER_BLOCK];

        let rbitmask = bitmask!(BITS_PER_SLOT);

        for i in 0..SLOTS_PER_TEST_BLOCK {
            assert_eq!(0, block.get_slot(BITS_PER_SLOT, i));

            let r = random_values[i] & rbitmask;

            //Set the value of r to the slot.  Note that we don't use the masked-out r we use the
            //full 64-bit random value.  This verifies that set_slot masks out the unused bits
            let prev_r = block.set_slot(BITS_PER_SLOT, i, random_values[i]);
            assert_eq!(0, prev_r);

            println!("After setting slot {} to value {:x}, block slots:", i, r);
            for j in 0..BITS_PER_SLOT {
                println!("Word {}: {:08x}", j, block[j]);
            }

            assert_eq!(r, block.get_slot(BITS_PER_SLOT, i));
        }

        for i in 0..SLOTS_PER_TEST_BLOCK {
            let r = random_values[i] & rbitmask;
            assert_eq!(r, block.get_slot(BITS_PER_SLOT, i));

            //Set this slot back to 0 and make sure that stuck
            let prev_r = block.set_slot(BITS_PER_SLOT, i, 0);

            assert_eq!(prev_r, r);
            assert_eq!(0, block.get_slot(BITS_PER_SLOT, i));
        }
    }

    #[test]
    pub fn find_slot_tests() {
        let bits_per_slot = BITS_PER_SLOT;
        let mut expected_word_index = 0usize;
        let mut expected_bit_offset_in_word = 0usize;
        let mut expected_bits_in_first_word = bits_per_slot;
        let mut expected_bits_in_second_word = 0usize;
        let block = [0u64; WORDS_PER_BLOCK];

        //Start with some easy ones; the first and the last slot in the array
        let (slot_word_index, slot_bit_offset_in_word, bits_in_first_word, bits_in_second_word) =
            find_slot(&block, BITS_PER_SLOT, 0);
        assert_eq!(0, slot_word_index);
        assert_eq!(0, slot_bit_offset_in_word);
        assert_eq!(BITS_PER_SLOT, bits_in_first_word);
        assert_eq!(0, bits_in_second_word);

        let (slot_word_index, slot_bit_offset_in_word, bits_in_first_word, bits_in_second_word) =
            find_slot(&block, BITS_PER_SLOT, SLOTS_PER_TEST_BLOCK - 1);
        assert_eq!(WORDS_PER_BLOCK - 1, slot_word_index);
        assert_eq!(64 - BITS_PER_SLOT, slot_bit_offset_in_word);
        assert_eq!(BITS_PER_SLOT, bits_in_first_word);
        assert_eq!(0, bits_in_second_word);

        for i in 0..SLOTS_PER_TEST_BLOCK {
            let (slot_word_index, slot_bit_offset_in_word, bits_in_first_word, bits_in_second_word) =
                find_slot(&block, BITS_PER_SLOT, i);

            assert_eq!(expected_word_index, slot_word_index, "slot index {}", i);
            assert_eq!(
                expected_bit_offset_in_word, slot_bit_offset_in_word,
                "slot index {}",
                i
            );
            assert_eq!(
                expected_bits_in_first_word, bits_in_first_word,
                "slot index {}",
                i
            );
            assert_eq!(
                expected_bits_in_second_word, bits_in_second_word,
                "slot index {}",
                i
            );

            //Compute what the expected value of the next slot will be, by just applying some
            //simple accumulation
            expected_bit_offset_in_word += bits_per_slot;
            if expected_bit_offset_in_word >= 64 {
                expected_word_index += 1;
                expected_bit_offset_in_word %= 64;
            }

            if expected_bit_offset_in_word + bits_per_slot <= 64 {
                expected_bits_in_first_word = bits_per_slot;
                expected_bits_in_second_word = 0;
            } else {
                expected_bits_in_first_word = 64 - expected_bit_offset_in_word;
                expected_bits_in_second_word = bits_per_slot - expected_bits_in_first_word;
            }
        }
    }

    #[test]
    pub fn find_slot_range_tests() {
        //let bits_per_slot = BITS_PER_SLOT;
        let block = [0u64; WORDS_PER_BLOCK];

        //As a first category of test inputs, find_slot_range with a count of 1 should always
        //produce equivalent results to find_slot
        for i in 0..SLOTS_PER_TEST_BLOCK {
            let (slot_word_index, _, bits_in_first_word, bits_in_second_word) =
                find_slot(&block, BITS_PER_SLOT, i);

            let (start_word_index, bits_in_start_word, end_word_index, bits_in_end_word) =
                find_slot_range(&block, BITS_PER_SLOT, i, 1);

            assert_eq!(slot_word_index, start_word_index, "slot {} count 1", i);
            assert_eq!(bits_in_first_word, bits_in_start_word, "slot {} count 1", i);

            if bits_in_second_word > 0 {
                assert_eq!(slot_word_index + 1, end_word_index, "slot {} count 1", i);
                assert_eq!(bits_in_second_word, bits_in_end_word, "slot {} count 1", i);
            } else {
                assert_eq!(slot_word_index, end_word_index, "slot {} count 1", i);
                assert_eq!(bits_in_first_word, bits_in_end_word, "slot {} count 1", i);
            }
        }

        //Try the entire array make sure it computes that properly
        let (start_word_index, bits_in_start_word, end_word_index, bits_in_end_word) =
            find_slot_range(&block, BITS_PER_SLOT, 0, SLOTS_PER_TEST_BLOCK);

        assert_eq!(0, start_word_index);
        assert_eq!(64, bits_in_start_word);
        assert_eq!(block.len() - 1, end_word_index);
        assert_eq!(64, bits_in_end_word);
    }
}
