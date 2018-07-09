//! Describes a `BitArray` trait and purpose-built implementations for both `u64` arrays and single
//! `u64` types.
use bitfiddling::BitFiddling;
use std::fmt;
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
    type WordType: Sized + fmt::Debug + fmt::LowerHex;

    #[inline]
    fn get_total_slots(&self, nbits: usize) -> usize;

    /// Gets the `nbits`-bit slot at `index` (`index` is the slot index, not the bit index)
    #[inline]
    fn get_slot(&self, nbits: usize, index: usize) -> Self::WordType;

    #[inline]
    /// Sets the `nbits`-bit slot at `index` (`index` is the slot index, not the bit index)
    ///
    /// # Returns
    ///
    /// Returns the previous value at index `index`.
    fn set_slot(&mut self, nbits: usize, index: usize, val: Self::WordType) -> Self::WordType;

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
        insert_value: Self::WordType,
    ) -> (usize, Self::WordType);

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
    fn insert_slot(
        &mut self,
        nbits: usize,
        index: usize,
        val: Self::WordType,
    ) -> Option<Self::WordType>;

    /// Deletes the `nbits`-bit slot at `index` (`index` is the slot index, not the bit index)
    ///
    /// "delete" here is the inverse of "insert"; subsequent slots will be shifted right.
    #[inline]
    fn delete_slot(&mut self, nbits: usize, index: usize) -> ();
}

// Private enum which represents the location of a slot of some number of bits within the words
// that make up the MultiBitArray.  Note the caller specifies the number of bits per slot; they are
// not captured here.
#[derive(Debug, PartialEq)]
#[allow(dead_code)]
enum SlotLocation {
    // Slot's bits are entirely in a single word (word_index, bit_index)
    SingleWord {
        word_index: usize,
        bit_index: usize,
    },

    // The slot's bits are spread across two words
    //  (start_word_index, start_bit_index)
    //
    // The index of the second word is always `start_word_index + 1`.  The number of bits in the
    // first word is `bits_per_word() - start_bit_index`, and the number of bits in the second word
    // is however many more bits after the bits in the first word are needed to make up all the
    // bits in a slot.
    SplitWords {
        start_word_index: usize,
        start_bit_index: usize,
    },
}

// Private enum which represents the possible forms a range over bits in a MultiBitArray can take
#[derive(Debug, PartialEq)]
#[allow(dead_code)]
enum SlotRangeLocation {
    /// Entire range of bits occurs in a single word
    SinglePartialWord {
        /// The index of the word containing this range
        word_index: usize,

        /// The index of the bit within the word where this range of bits starts
        start_bit_index: usize,

        /// The number of bits in this range
        bit_length: usize,
    },

    /// Entire range of bits encompases all bits of a single word
    WholeWords {
        /// The index of the first word containing this range.  The range starts at bit 0 of this
        /// word
        start_word_index: usize,

        /// The number of words in this range.  The range ends at the highest order bit of the word
        /// at offset start_word_index + word_length - 1
        word_length: usize,
    },

    /// Messy case where entire range spans two or more words a part of the either the first or last
    /// words, or possibly both first and last.
    ///
    /// (start_word, partial bits in first word (optional), number of whole words (optional),
    /// partial bits in last word (optional) )
    MultiplePartialWords {
        /// The index of the first word containing this range.  The range starts somewhere in this
        /// word
        start_word_index: usize,

        /// The number of bits of this range which are in the first word of the range, meaning the
        /// word at `start_word_index`.  If this is `None`, it means the range starts at bit 0 of
        /// the word at `start_word_index`; if it's not `None` then the range starts at
        /// `bits_per_word() - bits_in_partial_start_word` offset into the first word of the range.
        bits_in_partial_start_word: Option<usize>,

        /// The number of whole words containing this range, or `None` if there are no whole words
        /// in this range.
        whole_word_length: Option<usize>,

        /// The number of bits in this range which are in the last word of the range if the range
        /// ends somewhere before the highest bit of the last word of the range.
        bits_in_partial_end_word: Option<usize>,
    },
}

// Private trait which provides some internal-only helpers that are useful to implement the other
// methods
trait MultiBitArrayHelper: MultiBitArray {
    /// The total number of words in the backing array
    fn words_len(&self) -> usize;

    /// The number of bits in the word type
    fn bits_per_word() -> usize {
        mem::size_of::<Self::WordType>() * 8
    }

    /// Builds a vector with the contents of all of the slots of the array.  This is handy to
    /// visualize the contents of the array when debugging
    fn to_vector(&self, nbits: usize) -> Vec<Self::WordType> {
        let mut vec = Vec::with_capacity(self.get_total_slots(nbits));

        for slot in 0..vec.capacity() {
            vec.push(self.get_slot(nbits, slot));
        }

        vec
    }

    /// Returns a string representation of the vector representation of the array, also handy for
    /// debugging
    fn to_str(&self, nbits: usize) -> String {
        let vec = self.to_vector(nbits);

        //it's much easier to visualize bit-fiddly bugs if we see these slots in hex
        let hex_vec: Vec<String> = vec.iter().map(|number| format!("{:x}", number)).collect();

        format!(
            "{{ words_len: {}, nbits: {}, slots: [{:?}] }}",
            self.words_len(),
            nbits,
            hex_vec
        )
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
    fn find_slot(&self, nbits: usize, index: usize) -> (usize, usize, usize, usize) {
        debug_assert!(nbits > 0);

        let total_slots = self.get_total_slots(nbits);
        let block_word_size = Self::bits_per_word();
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

    /// Operates with `find_slot` but packages the results into a higher level Rust Enum type which
    /// makes the code that uses the slot location information more clear
    #[inline]
    fn find_slot_location(&self, nbits: usize, index: usize) -> SlotLocation {
        let (slot_word_index, slot_bit_offset_in_word, _, bits_in_second_word) =
            self.find_slot(nbits, index);

        if bits_in_second_word == 0 {
            //This slot is entirely in one word
            SlotLocation::SingleWord {
                word_index: slot_word_index,
                bit_index: slot_bit_offset_in_word,
            }
        } else {
            //This slot is straddling two words
            SlotLocation::SplitWords {
                start_word_index: slot_word_index,
                start_bit_index: slot_bit_offset_in_word,
            }
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
    /// * `start_bit_offset_in_word` - The bit offset within `start_word_index` where the range
    /// starts
    /// * `bits_in_start_word` - How many bits of `start_word_index` contain this range.  If the range
    /// spans multiple words, this will be the upper bits of that word.  
    /// * `end_word_index` - The index of the word that contains the last bit of slot `index+count-1`.
    /// If the range does not span multiple words, this is the same as `start_word_index`.
    /// * `bits_in_end_word` - The number of bits in `end_word_index` that contain this range.  If the
    /// range spans multiple words, these will be the lower bits of that word.
    #[inline]
    fn find_slot_range(
        &self,
        nbits: usize,
        start_index: usize,
        count: usize,
    ) -> (usize, usize, usize, usize, usize) {
        debug_assert!(nbits > 0);

        let total_slots = self.get_total_slots(nbits);
        let block_word_size = Self::bits_per_word();
        assert!(start_index + count <= total_slots);

        //Figure out where the first and last slot to be shifted are.
        let end_index = start_index + count - 1;
        let (start_word_index, start_bit_offset_in_word, _, _) = self.find_slot(nbits, start_index);

        let (
            end_word_index,
            end_bit_offset_in_word,
            end_bits_in_first_word,
            end_bits_in_second_word,
        ) = self.find_slot(nbits, end_index);

        //how many bits, total, are in the start word?
        if start_word_index == end_word_index && end_bits_in_second_word == 0 {
            let total_bits = count * nbits;

            // in debug builds sanity-check this total; this expression and the one used for total_bits
            // should be equivalent
            debug_assert!(
                total_bits
                    == end_bit_offset_in_word + end_bits_in_first_word - start_bit_offset_in_word
            );

            return (
                start_word_index,
                start_bit_offset_in_word,
                total_bits,
                end_word_index,
                total_bits,
            );
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
            start_bit_offset_in_word,
            bits_in_start_word,
            end_word_index,
            bits_in_end_word,
        )
    }

    /// Translates the results of `find_slot_range` high a higher level package using Rust Enums to
    /// make to easier to operate on such ranges of bits
    #[inline]
    fn find_slot_range_location(
        &self,
        nbits: usize,
        start_index: usize,
        count: usize,
    ) -> SlotRangeLocation {
        //This can be implemented generically for all underlying array types
        let block_word_size = Self::bits_per_word();

        let (
            start_word_index,
            start_bit_offset_in_word,
            bits_in_start_word,
            end_word_index,
            bits_in_end_word,
        ) = self.find_slot_range(nbits, start_index, count);

        if start_word_index == end_word_index && bits_in_start_word < block_word_size {
            //This range is entirely in one word, but not the ENTIRE word some subset of it
            return SlotRangeLocation::SinglePartialWord {
                word_index: start_word_index,
                start_bit_index: start_bit_offset_in_word,
                bit_length: bits_in_start_word,
            };
        } else if bits_in_start_word == block_word_size && bits_in_end_word == block_word_size {
            //This range encompases all of the bits in one or more words
            return SlotRangeLocation::WholeWords {
                start_word_index: start_word_index,
                word_length: end_word_index - start_word_index + 1,
            };
        } else {
            //This range covers more than one word, but either the start or end or both ends of the
            //range are in part of a word.
            let total_words = end_word_index - start_word_index + 1;
            let (partial_start_bits, partial_end_bits, whole_words) = if bits_in_start_word
                < block_word_size
                && bits_in_end_word < block_word_size
            {
                //This range starts and ends on a partial word
                (
                    Some(bits_in_start_word),
                    Some(bits_in_end_word),
                    total_words - 2,
                )
            } else if bits_in_start_word < block_word_size {
                //This range starts on a partial word and ends on a whole word
                (Some(bits_in_start_word), None, total_words - 1)
            } else {
                //This range must start on a whole word and end on a partial word
                //We know this because if it started AND ended on a whole word the if clause that
                //returns a WholeWords value would have caught it already
                debug_assert!(bits_in_start_word == block_word_size);
                debug_assert!(bits_in_end_word < block_word_size);
                (None, Some(bits_in_end_word), total_words - 1)
            };

            return SlotRangeLocation::MultiplePartialWords {
                start_word_index: start_word_index,
                bits_in_partial_start_word: partial_start_bits,
                whole_word_length: if whole_words > 0 {
                    Some(whole_words)
                } else {
                    None
                },
                bits_in_partial_end_word: partial_end_bits,
            };
        }
    }

    /// Performs a partial shift operation on a subset of the bits in a word, returns the value
    /// of the bits that are shifted off the word.  In the opening made by the shift, puts
    /// `shift_value`
    #[inline]
    fn shift_left_part_of_word(
        &mut self,
        word_index: usize,
        start_bit_offset_in_word: usize,
        count_bits: usize,
        nbits: usize,
        insert_value: Self::WordType,
    ) -> Self::WordType;

    /// Shifts an entire range of words left by `nbits`, inserting `insert_value` into the
    /// start of the first word and returning the bits shifted off the end of the last word
    #[inline]
    fn shift_left_entire_words(
        &mut self,
        start_word_index: usize,
        word_count: usize,
        nbits: usize,
        insert_value: Self::WordType,
    ) -> Self::WordType;
}

/// Specialized implementation of MultiBitArray for slices of `u64`.  We expect this to actually be
/// a fixed-length array of `u64` but it's more convenient to specify in terms of a slice.  If this
/// introduces an excessive performance penalty then we'll have to use macros and generate
/// implementations for various possible sizes of `u64` arrays.  
impl MultiBitArray for [u64] {
    type WordType = u64;

    fn get_total_slots(&self, nbits: usize) -> usize {
        self.len() * 64 / nbits
    }

    #[inline]
    fn get_slot(&self, nbits: usize, index: usize) -> Self::WordType {
        match self.find_slot_location(nbits, index) {
            SlotLocation::SingleWord {
                word_index,
                bit_index,
            } => {
                //All of the slot is in one word; this statistically is the more common case
                (self[word_index] >> bit_index) & bitmask!(nbits)
            }

            SlotLocation::SplitWords {
                start_word_index,
                start_bit_index,
            } => {
                //This slot starts on one word and ends on the following word so extract both pieces
                //then put them together
                //If a slot spans two words, we know the the part of the slot's value in the second
                //word starts at bit 0 in that word, so unlike for the first part which requires a bit
                //shift and then a bit mask, the second part can be obtained with just a bit mask
                let bits_in_first_word = Self::bits_per_word() - start_bit_index;
                let bits_in_second_word = nbits - bits_in_first_word;
                let first_part =
                    (self[start_word_index] >> start_bit_index) & bitmask!(bits_in_first_word);
                let second_part = self[start_word_index + 1] & bitmask!(bits_in_second_word);

                first_part | (second_part << bits_in_first_word)
            }
        }
    }

    #[inline]
    fn set_slot(&mut self, nbits: usize, index: usize, val: Self::WordType) -> Self::WordType {
        match self.find_slot_location(nbits, index) {
            SlotLocation::SingleWord {
                word_index,
                bit_index,
            } => {
                //This slot is entirely contained within one word
                let old_word = self[word_index];
                let slot_bitmask = bitmask!(nbits) << bit_index;
                let new_word = (old_word & !slot_bitmask) | ((val << bit_index) & slot_bitmask);

                self[word_index] = new_word;

                //Return the previous value of this slot
                ((old_word & slot_bitmask) >> bit_index)
            }

            SlotLocation::SplitWords {
                start_word_index,
                start_bit_index,
            } => {
                //This slot starts on one word and ends on the following word so extract both pieces
                //and then put them in the two words

                //If a slot spans two words, we know the the part of the slot's value in the second
                //word starts at bit 0 in that word, so unlike for the first part which requires a bit
                //shift and then a bit mask, the second part can be obtained with just a bit mask
                let bits_in_first_word = Self::bits_per_word() - start_bit_index;
                let bits_in_second_word = nbits - bits_in_first_word;
                let old_word = self[start_word_index];
                let slot_bitmask = bitmask!(bits_in_first_word) << start_bit_index;
                let first_part = (old_word & slot_bitmask) >> start_bit_index;
                let new_word =
                    (old_word & !slot_bitmask) | ((val << start_bit_index) & slot_bitmask);

                self[start_word_index] = new_word;

                let old_word = self[start_word_index + 1];
                let slot_bitmask = bitmask!(bits_in_second_word);
                let second_part = old_word & slot_bitmask;
                let new_word =
                    (old_word & !slot_bitmask) | ((val >> bits_in_first_word) & slot_bitmask);

                self[start_word_index + 1] = new_word;

                // Finally, return the previous value
                first_part | (second_part << bits_in_first_word)
            }
        }
    }

    #[allow(unused_variables)]
    #[inline]
    fn shift_slots_left(
        &mut self,
        nbits: usize,
        start_index: usize,
        slot_count: usize,
        insert_value: Self::WordType,
    ) -> (usize, Self::WordType) {
        debug_assert!(slot_count > 0);
        debug_assert!((insert_value & !bitmask!(nbits)) == 0);

        //`slot_count` is allowed to be higher than the number of slots in this array; for this
        //implementation clamp it at the max for this array and return the number of slots left
        //for the caller to process itself
        let local_slot_count = if start_index + slot_count > self.get_total_slots(nbits) {
            self.get_total_slots(nbits) - start_index
        } else {
            slot_count
        };

        match self.find_slot_range_location(nbits, start_index, local_slot_count) {
            SlotRangeLocation::SinglePartialWord {
                word_index,
                start_bit_index,
                bit_length,
            } => {
                debug_assert!(local_slot_count * nbits == bit_length);

                let shifted_slot = self.shift_left_part_of_word(
                    word_index,
                    start_bit_index, //start shifting bits at this bit offset
                    bit_length,      //this many bits in the word will be modified by this operation
                    nbits,           //shift the bits by nbits, the number of bits in one slot
                    insert_value, //in the opening made by this operation, insert this value (assumed to have nbits)
                );

                (slot_count - local_slot_count, shifted_slot)
            }

            SlotRangeLocation::WholeWords {
                start_word_index,
                word_length,
            } => {
                //This is an easy and high performance case; just shift all of the words by their
                //entire contents
                let shifted_slot = self.shift_left_entire_words(
                    start_word_index,
                    word_length,
                    nbits,
                    insert_value,
                );

                // That's it; shifted the entire block left by nbits; shifted_slot contains the value
                // of the top-most slot in the block which should be shifted into the bottom of the
                // next block.
                (slot_count - local_slot_count, shifted_slot)
            }

            SlotRangeLocation::MultiplePartialWords {
                start_word_index,
                bits_in_partial_start_word,
                whole_word_length,
                bits_in_partial_end_word,
            } => {
                //This is the most complex case.  Either the start or end or both ends of this
                //range fall within a word, not on a word boundary.  There might be some whole
                //words in between the two ends, or this range might just straddle two words.
                //
                //The algorithm used here is to perform a whole-word bitwise shift on all of the
                //words which include this range, including the partial start and/or end.  We'll
                //save copies of those partial words so we can fix up the results afterward.

                //Figure out how many words are involved in this range, including whole words and
                //partial words
                let total_word_length = bits_in_partial_start_word.map_or(0, |_| 1)
                    + bits_in_partial_end_word.map_or(0, |_| 1)
                    + whole_word_length.unwrap_or(0);

                // Compute thew new shifted value of the start word if only part of it includes the
                // range.  This needs to be cone before we shift all of the words below.
                let new_start_word = bits_in_partial_start_word.map(|partial_bits| {
                    let word = self[start_word_index];
                    let start_bit_index = Self::bits_per_word() - partial_bits;

                    //Perform the shift on only the subset of this first word that contains the
                    //range.
                    //
                    //Note we don't care about the word-level shifted value because that'll be
                    //computed below when we bulk-shift all the words
                    let (new_word, _) = word.shift_left_into_partial(
                        start_bit_index,
                        partial_bits,
                        nbits,
                        insert_value,
                    );

                    new_word
                });

                //Just as we compute the new start word before the shift, the end is the opposite;
                //we need to make a backup of the end word before the shift, and the mask we'll use
                //to restore only the part of the end word that doesn't include the range.
                let old_end_word_and_mask = bits_in_partial_end_word.map(|partial_bits| {
                    //Make a mask that will cover all of the bits in the end word that are NOT part
                    //of the range.
                    let preserve_mask = bitmask!(Self::bits_per_word()) << partial_bits;

                    //Keep a copy of that mask and also the value of those bits of the end word
                    let old_end_word = self[start_word_index + total_word_length - 1];

                    (old_end_word, preserve_mask)
                });

                //If the end of this range is at the end of the end word, then when we shift entire
                //words the `shifted_slot` will be acurate.  If not, we need to figure out the
                //value in the slot that is going to be shifted off this range.
                //TODO: Do this in some clever and performant way.  For now just use `get_slot` to
                //read this slot out.
                let partial_end_shifted_slot = bits_in_partial_end_word.map(|_| {
                    //The range doesn't end on a word boundary, so need to pull out the shifted
                    //slot manually.
                    self.get_slot(nbits, start_index + slot_count - 1)
                });

                //Apply the shift operation to all of the words which include the range, partilly
                //or wholly
                let aligned_end_shifted_slot = self.shift_left_entire_words(
                    start_word_index,
                    total_word_length,
                    nbits,
                    insert_value,
                );

                //If there was a partial start word, overwrite the start word which was part of the
                //total shift with the value we computed specially
                new_start_word.map(|new_word| {
                    self[start_word_index] = new_word;
                });

                //If there was a partial end word, use the mask and backup copy we made above and
                //apply that over the end word computed as part of the total shift
                old_end_word_and_mask.map(|(old_word, preserve_mask)| {
                    let new_word = self[start_word_index + total_word_length - 1];

                    self[start_word_index + total_word_length - 1] =
                        (new_word & !preserve_mask) | (old_word & preserve_mask);
                });

                //Figure out what the shifted slot will be; which one we use depends on whether or
                //not the end of the range is word aligned.
                let shifted_slot = partial_end_shifted_slot.unwrap_or(aligned_end_shifted_slot);

                //Phew!  Doesn't seem like it should be that hard, but we're done now.
                (slot_count - local_slot_count, shifted_slot)
            }
        }
    }

    #[inline]
    #[allow(unused_variables)]
    #[allow(dead_code)]
    fn insert_slot(
        &mut self,
        nbits: usize,
        index: usize,
        val: Self::WordType,
    ) -> Option<Self::WordType> {
        panic!("NYI");
    }

    #[inline]
    #[allow(unused_variables)]
    #[allow(dead_code)]
    fn delete_slot(&mut self, nbits: usize, index: usize) -> () {
        panic!("NYI");
    }
}

impl MultiBitArrayHelper for [u64] {
    fn words_len(&self) -> usize {
        self.len()
    }

    /// Performs a partial shift operation on a subset of the bits in a word, returns the value
    /// of the bits that are shifted off the word.  In the opening made by the shift, puts
    /// `shift_value`
    #[inline]
    fn shift_left_part_of_word(
        &mut self,
        word_index: usize,
        start_bit_offset_in_word: usize,
        count_bits: usize,
        nbits: usize,
        insert_value: Self::WordType,
    ) -> Self::WordType {
        let word = self[word_index];
        let (new_word, shifted_value) = word.shift_left_into_partial(
            start_bit_offset_in_word, //start shifting bits at this bit offset
            count_bits,   //this many bits in the word will be modified by this operation
            nbits,        //shift the bits by nbits, the number of bits in one slot
            insert_value, //in the opening made at start_bit_offset_in_word by this operation, insert this value (assumed to have nbits)
        );

        self[word_index] = new_word;

        shifted_value
    }

    /// Shifts an entire range of words left by `nbits`, inserting `insert_value` into the
    /// start of the first word and returning the bits shifted off the end of the last word
    #[inline]
    fn shift_left_entire_words(
        &mut self,
        start_word_index: usize,
        word_count: usize,
        nbits: usize,
        insert_value: Self::WordType,
    ) -> Self::WordType {
        let mut shifted_value = insert_value;

        for i in start_word_index..start_word_index + word_count {
            let word = self[i];

            //Rust as of now doesn't support destructuring assignment, so capture the tuple as
            //a variable and destructure it manually
            let shift_results = word.shift_left_into(nbits, shifted_value);

            self[i] = shift_results.0;
            shifted_value = shift_results.1;
        }

        shifted_value
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
    pub fn shift_slots_left_test() {
        let mut block = [0u64; WORDS_PER_BLOCK];

        //Simple tests, operating on ranges that are entirely within one word.
        const WORD_VALUE: u64 = 0xffee_ddcc_bbaa_9900;
        let bits_per_slot = 8; //this is easier visually than BITS_PER_SLOT which is 9

        block[0] = WORD_VALUE;

        //Starting at slot 0, shift just one slot left, and put 0x55 in its place.
        let (remaining_slots, shifted_value) = block.shift_slots_left(bits_per_slot, 0, 1, 0x55);

        assert_eq!(0xffee_ddcc_bbaa_9955, block[0]);
        assert_eq!(0x00, shifted_value);
        assert_eq!(0, remaining_slots);

        block[0] = WORD_VALUE;

        //Let's make it more interesting: shift multiple slots
        let (remaining_slots, shifted_value) = block.shift_slots_left(bits_per_slot, 0, 3, 0x55);

        assert_eq!(0xffee_ddcc_bb99_0055, block[0]);
        assert_eq!(0xaa, shifted_value);
        assert_eq!(0, remaining_slots);

        block[0] = WORD_VALUE;

        //still more interesting: a non-zero starting offset
        let (remaining_slots, shifted_value) = block.shift_slots_left(bits_per_slot, 1, 3, 0x55);

        assert_eq!(0xffee_ddcc_aa99_5500, block[0]);
        assert_eq!(0xbb, shifted_value);
        assert_eq!(0, remaining_slots);

        block[0] = WORD_VALUE;

        //Shift the entire word
        let (remaining_slots, shifted_value) =
            block.shift_slots_left(bits_per_slot, 0, 64 / bits_per_slot, 0x55);

        assert_eq!(0xeedd_ccbb_aa99_0055, block[0]);
        assert_eq!(0xff, shifted_value);
        assert_eq!(0, remaining_slots);

        block[0] = WORD_VALUE;
    }

    #[test]
    pub fn shift_slots_left_torture_test() {
        //This test exercises all three types of ranges in the shift logic.  It maintains two
        //multibitarrays: one implements the shift manually with get_slot/set_slot calls, the other
        //uses shift_slots_left, and after each operation we expect the results to match.
        fn get_slot_value(i: usize) -> u64 {
            (i as u64) & bitmask!(BITS_PER_SLOT)
        }

        // This does in the native way what all these hundreds of lines of code are trying to do in
        // an optimized way
        fn shift_slots_left_slow(
            block: &mut [u64],
            slot_index: usize,
            slot_count: usize,
            insert_value: u64,
        ) -> u64 {
            assert!(slot_index + slot_count <= block.get_total_slots(BITS_PER_SLOT));

            let mut shifted_value = insert_value;

            for i in slot_index..(slot_index + slot_count) {
                shifted_value = block.set_slot(BITS_PER_SLOT, i, shifted_value);
            }

            shifted_value
        }

        let mut test_block = [0u64; WORDS_PER_BLOCK];
        assert_eq!(
            WORDS_PER_BLOCK * 64 / BITS_PER_SLOT,
            test_block.get_total_slots(BITS_PER_SLOT)
        );
        for i in 0..test_block.get_total_slots(BITS_PER_SLOT) {
            let slot_value = get_slot_value(i);

            test_block.set_slot(BITS_PER_SLOT, i, slot_value);
        }

        //This will no longer be mutable
        let test_block = test_block;

        // Each element is a test case.
        //
        // (slot_index, slot_count, insert_value)
        let test_scenarios = [
            // Cover the entire array
            (0, test_block.get_total_slots(BITS_PER_SLOT), 0xff),
            // Cover an area within a single word
            (1, 5, 0xff),
            // Cover an area that spans two words
            (1, 12, 0xff),
            //Cover an area that starts on a word boundary but finishes part way into another word
            (0, 10, 0xff),
            //Cover an area that starts in the middle of a word but finishes on a word boundary
            (1, 63, 0xff),
            //Cover an area that starts and ends on a word boundary
            (0, 64, 0xff),
        ];

        for (slot_index, slot_count, insert_value) in &test_scenarios {
            println!(
                "Test case: *slot_index={} *slot_count={} *insert_value={}",
                *slot_index, *slot_count, *insert_value
            );
            let mut expected_block = [0u64; WORDS_PER_BLOCK];
            let mut actual_block = [0u64; WORDS_PER_BLOCK];

            expected_block.copy_from_slice(&test_block);
            actual_block.copy_from_slice(&test_block);

            let expected_shifted_slot =
                shift_slots_left_slow(&mut expected_block, *slot_index, *slot_count, *insert_value);
            let (remaining_slots, actual_shifted_slot) = actual_block.shift_slots_left(
                BITS_PER_SLOT,
                *slot_index,
                *slot_count,
                *insert_value,
            );

            assert_eq!(0, remaining_slots);

            assert_blocks_eq(
                BITS_PER_SLOT,
                &expected_block,
                &actual_block,
                &format!(
                    "*slot_index={} *slot_count={} *insert_value={}",
                    *slot_index, *slot_count, *insert_value
                ),
            );

            assert_eq!(
                expected_shifted_slot, actual_shifted_slot,
                "*slot_index={} *slot_count={} *insert_value={}",
                *slot_index, *slot_count, *insert_value
            );
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
            block.find_slot(BITS_PER_SLOT, 0);
        assert_eq!(0, slot_word_index);
        assert_eq!(0, slot_bit_offset_in_word);
        assert_eq!(BITS_PER_SLOT, bits_in_first_word);
        assert_eq!(0, bits_in_second_word);

        let (slot_word_index, slot_bit_offset_in_word, bits_in_first_word, bits_in_second_word) =
            block.find_slot(BITS_PER_SLOT, SLOTS_PER_TEST_BLOCK - 1);
        assert_eq!(WORDS_PER_BLOCK - 1, slot_word_index);
        assert_eq!(64 - BITS_PER_SLOT, slot_bit_offset_in_word);
        assert_eq!(BITS_PER_SLOT, bits_in_first_word);
        assert_eq!(0, bits_in_second_word);

        for i in 0..SLOTS_PER_TEST_BLOCK {
            let (slot_word_index, slot_bit_offset_in_word, bits_in_first_word, bits_in_second_word) =
                block.find_slot(BITS_PER_SLOT, i);

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

            //Now call find_slot_location and confirm it reports the slot location in a useful
            //package
            let loc = block.find_slot_location(BITS_PER_SLOT, i);

            if expected_bits_in_second_word == 0 {
                assert_eq!(
                    SlotLocation::SingleWord {
                        word_index: expected_word_index,
                        bit_index: expected_bit_offset_in_word
                    },
                    loc
                );
            } else {
                assert_eq!(
                    SlotLocation::SplitWords {
                        start_word_index: expected_word_index,
                        start_bit_index: expected_bit_offset_in_word
                    },
                    loc
                );
            }

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
            let (slot_word_index, slot_bit_offset_in_word, bits_in_first_word, bits_in_second_word) =
                block.find_slot(BITS_PER_SLOT, i);

            let (
                start_word_index,
                start_bit_offset_in_word,
                bits_in_start_word,
                end_word_index,
                bits_in_end_word,
            ) = block.find_slot_range(BITS_PER_SLOT, i, 1);

            assert_eq!(slot_word_index, start_word_index, "slot {} count 1", i);
            assert_eq!(
                slot_bit_offset_in_word, start_bit_offset_in_word,
                "slot {} count 1",
                i
            );
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
        let (
            start_word_index,
            _start_bit_offset_in_word,
            bits_in_start_word,
            end_word_index,
            bits_in_end_word,
        ) = block.find_slot_range(BITS_PER_SLOT, 0, SLOTS_PER_TEST_BLOCK);

        assert_eq!(0, start_word_index);
        assert_eq!(64, bits_in_start_word);
        assert_eq!(block.len() - 1, end_word_index);
        assert_eq!(64, bits_in_end_word);
    }

    #[test]
    pub fn find_slot_range_location_tests() {
        let block = [0u64; WORDS_PER_BLOCK];

        //As a first category of test inputs, find_slot_range with a count of 1 should always
        //produce equivalent results to find_slot
        for i in 0..SLOTS_PER_TEST_BLOCK {
            let loc = block.find_slot_range_location(BITS_PER_SLOT, i, 1);

            let (
                slot_word_index,
                bit_offset_in_first_word,
                bits_in_first_word,
                bits_in_second_word,
            ) = block.find_slot(BITS_PER_SLOT, i);

            if bits_in_second_word > 0 {
                assert_eq!(
                    SlotRangeLocation::MultiplePartialWords {
                        start_word_index: slot_word_index,
                        bits_in_partial_start_word: Some(bits_in_first_word),
                        bits_in_partial_end_word: Some(bits_in_second_word),
                        whole_word_length: None
                    },
                    loc,
                    "slot {} count 1",
                    i
                );
            } else {
                assert_eq!(
                    SlotRangeLocation::SinglePartialWord {
                        word_index: slot_word_index,
                        start_bit_index: bit_offset_in_first_word,
                        bit_length: BITS_PER_SLOT
                    },
                    loc,
                    "slot {} count 1",
                    i
                );
            }
        }

        //Try a range that includes the entire array
        let loc =
            block.find_slot_range_location(BITS_PER_SLOT, 0, block.get_total_slots(BITS_PER_SLOT));
        assert_eq!(
            SlotRangeLocation::WholeWords {
                start_word_index: 0,
                word_length: block.words_len()
            },
            loc
        );
    }

    fn assert_blocks_eq(bits_per_slot: usize, expected: &[u64], actual: &[u64], context: &str) {
        assert_eq!(expected.len(), actual.len(), "context: {}", context);

        for i in 0..expected.get_total_slots(bits_per_slot) {
            assert_eq!(
                expected.get_slot(bits_per_slot, i),
                actual.get_slot(bits_per_slot, i),
                "slot={} context: {}",
                i,
                context,
            );
        }
    }
}
