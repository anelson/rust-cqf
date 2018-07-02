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
    let total_slots = arr.len() * 64 / nbits;
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
}
