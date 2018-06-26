use murmur::Murmur3Hash;
use std::mem::size_of;
use std::vec::Vec;

const BITS_PER_SLOT: usize = 9; //corresponds to false positive rate of 1/(2^9) == 1/512
const BLOCK_OFFSET_BITS: usize = (6); //6 seems fastest; corresponds to max offset 2^6-1 or 63
const SLOTS_PER_BLOCK: usize = (1 << BLOCK_OFFSET_BITS); //64 currently
const BLOCK_WORD_SIZE: usize = size_of::<u64>() * 8; //64 bits
const METADATA_WORDS_PER_BLOCK: usize = ((SLOTS_PER_BLOCK + BLOCK_WORD_SIZE - 1) / BLOCK_WORD_SIZE); //64 bits per word

#[allow(dead_code)] // for now
pub struct RSQF {
    meta: Metadata,
    logical: LogicalData,
}

#[allow(dead_code)] // for now
#[derive(Default, PartialEq)]
struct Metadata {
    n: usize,
    qbits: usize,
    rbits: usize,
    nblocks: usize,
    nelements: usize,
    ndistinct_elements: usize,
    nslots: usize,
    noccupied_slots: usize,
    max_slots: usize,
}

#[allow(dead_code)] // for now
#[derive(Default, PartialEq)]
struct LogicalData {
    physical: PhysicalData,
}

#[allow(dead_code)] // for now
#[derive(Default, PartialEq)]
struct PhysicalData {
    blocks: Vec<Block>,
    rbits: usize,
}

#[allow(dead_code)] // for now
#[derive(Clone, Copy, Default, PartialEq, Debug)]
struct Block {
    offset: u8,
    occupieds: [u64; METADATA_WORDS_PER_BLOCK],
    runends: [u64; METADATA_WORDS_PER_BLOCK],
    slots: [u64; BITS_PER_SLOT],
}

/// Standard filter result type, on success returns a count on error returns a message
/// This should probably be richer over time
type FilterResult = Result<usize, &'static str>;

#[allow(dead_code)] // for now
#[allow(unused_variables)] // for now
impl RSQF {
    pub fn new(n: usize, rbits: usize) -> RSQF {
        RSQF::from_n_and_r(n, rbits)
    }

    /// Creates a structure for the filter based on `n` the number of expected elements
    /// and `rbits` which specifies the false positive rate at `1/(2^rbits - 1)`
    fn from_n_and_r(n: usize, rbits: usize) -> RSQF {
        RSQF::from_metadata(Metadata::from_n_and_r(n, rbits))
    }

    /// Creates an instance of the filter given the description of the filter parameters stored in
    /// a `Metadata` structure
    fn from_metadata(meta: Metadata) -> RSQF {
        let logical = LogicalData::new(&meta);

        return RSQF { meta, logical };
    }

    /// Queries the filter for the presence of `hash`.
    ///
    /// If `hash` is not present, returns 0
    /// If `hash` is likely to be present, returns an approximate count of the number of times
    /// `hash` has been inserted.  Note that this is approximate; it is possible that `hash` is
    /// actually not present but a non-zero count is returned, with a probability no worse than
    /// `2^-rbits`
    pub fn get_count(&self, hash: Murmur3Hash) -> usize {
        panic!("NYI");
    }

    /// Adds `count` to the total count for `hash` in the filter.
    ///
    /// If `hash` is not present in the filter, it is added with `count` count.
    /// If `hash` is present, `count` is added to its existing count.
    ///
    /// As with the `query` method it is possible `hash` collides with another hash value
    ///
    /// Returns the new total count of `hash` on success, or `Err` if the filter is already at max
    /// capacity
    pub fn add_count(&self, hash: Murmur3Hash, count: usize) -> FilterResult {
        panic!("NYI");
    }

    /// Increments the count of `hash` by one.
    ///
    /// If `hash` is not present, it's added with a count of one.
    /// If `hash` is present, its existing count is incremented.
    ///
    /// Returns the new total count of `hash` on success, or `Err` if the filter is already at max
    /// capacity
    pub fn inc_count(&self, hash: Murmur3Hash) -> FilterResult {
        return self.add_count(hash, 1);
    }

    /// Subtracts `count` from the total count for `hash` in the filter.
    ///
    /// If `hash` is not present in the filter, an error is returned
    /// If `hash` is present, `count` is subtracted from the existing count.  The resulting count
    /// is returned.  If subtracting `count` from the existing count results in a value less than
    /// 1, a resulting count of 0 is returned and `hash` is removed from the filter.
    ///
    /// As with the `query` method it is possible `hash` collides with another hash value
    ///
    /// Returns the new total count of `hash` on success, which may be 0 in which case `hash` has
    /// been removed from the filter, or an error if `hash` was not found in the filter
    pub fn sub_count(&self, hash: Murmur3Hash, count: usize) -> FilterResult {
        panic!("NYI");
    }

    pub fn dec_count(&self, hash: Murmur3Hash) -> FilterResult {
        return self.sub_count(hash, 1);
    }

    /// Given a Murmur3 hash as input, extracts the quotient `q` and remainder `r` which will be
    /// used to look up this item in the filter.
    ///
    /// Though both values are of type `u64`, the number of bits used in each is based on the size
    /// (`n`) and false-positive rate (`rbits`) specified when the filter was created
    fn get_q_and_r(&self, hash: Murmur3Hash) -> (u64, u64) {
        //Use only the 64-bit hash and pull out the bits we'll use for q and r
        let hash = hash.value64();

        // To compute the quotient q for this hash, shift right to remove the bits to be used as
        // the remainder r, then mask out q bits
        let q = (hash.wrapping_shr(self.meta.rbits as u32)) & bitmask!(self.meta.qbits);
        let r = hash & bitmask!(self.meta.rbits as u32);

        (q, r)
    }
}

#[cfg(test)]
mod rsqf_tests {
    use super::*;
    use murmur::Murmur3Hash;

    #[test]
    fn creates_empty_filter() {
        let _filter = RSQF::new(10000, 9);
    }

    #[test]
    #[should_panic]
    fn panics_on_invalid_r() {
        RSQF::new(10000, 8);
    }

    #[test]
    fn computes_valid_metadata() {
        let filter = RSQF::new(10000, 9);

        assert_eq!(filter.meta.n, 10000);
        assert_eq!(filter.meta.rbits, 9);
        assert_eq!(filter.meta.qbits, 14);
        assert_eq!(filter.meta.nslots, 1usize << 14);
        assert_eq!(filter.meta.nblocks, (filter.meta.nslots + 64 - 1) / 64);
        assert_eq!(filter.meta.noccupied_slots, 0);
        assert_eq!(filter.meta.nelements, 0);
        assert_eq!(filter.meta.ndistinct_elements, 0);
        assert_eq!(
            filter.meta.max_slots,
            ((filter.meta.nslots as f64) * 0.95) as usize
        );
    }

    #[test]
    #[ignore]
    fn get_count_nonexistent_item_returns_zero() {
        let filter = RSQF::new(10000, 9);

        assert_eq!(0, filter.get_count(Murmur3Hash::new(1)));
    }

    #[test]
    fn get_q_and_r_returns_correct_results() {
        let test_data = [
            // (n, rbits, hash)
            (30usize, 9usize, 0x0000_0000u128),
            (30usize, 9usize, 0b0000_0001_1111_1111u128),
            (30usize, 9usize, 0b1111_0001_1111_0000u128),
        ];

        for (n, rbits, hash) in test_data.into_iter() {
            let filter = RSQF::new(*n, *rbits);

            println!(
                "n={} qbits={} rbits={} hash={:x}",
                n, filter.meta.qbits, rbits, hash
            );

            let hash = Murmur3Hash::new(*hash);

            let (q, r) = filter.get_q_and_r(hash);

            println!("q={:x}", q);
            println!("r={:x}", r);

            let rbitmask = u128::max_value() >> (128 - *rbits);
            let qbitmask = u128::max_value() >> (128 - filter.meta.qbits);

            //The lower rbits bits of the hash should be r
            assert_eq!(hash.value128() & rbitmask, r as u128);
            assert_eq!((hash.value128() >> rbits) & qbitmask, q as u128);
        }
    }
}

#[allow(dead_code)] // for now
#[allow(unused_variables)] // for now
impl Metadata {
    /// Creates a metadata structure for the filter based on `n` the number of expected elements
    /// and `rbits` which specifies the false positive rate at `1/(2^rbits - 1)`
    fn from_n_and_r(n: usize, rbits: usize) -> Metadata {
        assert!(SLOTS_PER_BLOCK == 64usize); //this code assumes 64 slots per block always
        assert!(rbits as usize == BITS_PER_SLOT); //TODO: figure out how to make this configurable

        let qbits = Metadata::calculate_qbits(n, rbits);
        let total_slots = 1usize << qbits; //2^qbits slots in the filter
        let nblocks = (total_slots + SLOTS_PER_BLOCK - 1) / SLOTS_PER_BLOCK;

        //Conservatively, set the maximum number of elements to 95% of the total capacity
        //Realistically this structure can go higher than that but there starts to be a performance
        //penalty and it's better to resize at that point
        let max_slots = ((total_slots as f64) * 0.95) as usize;

        return Metadata {
            n,
            rbits,
            qbits,
            nblocks,
            max_slots,
            nslots: total_slots,
            ..Default::default()
        };
    }

    /// Given the insert count `n` and the remainder bits `rbits`, calculates the quotient size
    /// `qbits` which will provide a false positive rate of no worse than `1/(2^rbits - 1)`
    fn calculate_qbits(n: usize, rbits: usize) -> usize {
        assert!(rbits > 1);
        assert!(n > 0);

        let sigma = 2.0f64.powi(-(rbits as i32));
        let p = ((n as f64) / sigma).log2().ceil() as usize;

        assert!(p > rbits);

        let qbits = p - rbits;

        qbits
    }
}

#[cfg(test)]
mod metadata_tests {
    use super::*;

    #[test]
    #[should_panic]
    fn panics_on_invalid_rbits() {
        Metadata::from_n_and_r(10000, 8);
    }

    #[test]
    fn computes_valid_q_for_n_and_r() {
        // Test data data values were computed from a Google Sheet using formulae from the RSQF
        // paper
        let test_data = [
            // (n, r, expected_q)
            (100_000_usize, 6_usize, 17),
            (1_000_000_usize, 6_usize, 20),
            (10_000_000_usize, 6_usize, 24),
            (100_000_usize, 8_usize, 17),
            (1_000_000_usize, 8_usize, 20),
            (10_000_000_usize, 8_usize, 24),
            (100_000_usize, 9_usize, 17),
            (1_000_000_usize, 9_usize, 20),
            (10_000_000_usize, 9_usize, 24),
        ];

        for (n, r, expected_qbits) in test_data.into_iter() {
            let q = Metadata::calculate_qbits(*n, *r);
            assert_eq!(*expected_qbits, q, "n={} r={}", *n, *r);
        }
    }

    #[test]
    fn computes_valid_metadata_for_n_and_r() {
        let test_data = [
            // (n, r, expected_qbits, expected_nslots)
            (10_000_usize, 9_usize, 14, 1usize << 14),
        ];

        for (n, r, expected_qbits, expected_nslots) in test_data.into_iter() {
            let meta = Metadata::from_n_and_r(*n, *r);

            assert_eq!(meta.n, *n);
            assert_eq!(meta.rbits, *r);
            assert_eq!(meta.qbits, *expected_qbits);
            assert_eq!(meta.nslots, *expected_nslots);
            assert_eq!(meta.nblocks, (meta.nslots + 64 - 1) / 64);
            assert_eq!(meta.noccupied_slots, 0);
            assert_eq!(meta.nelements, 0);
            assert_eq!(meta.ndistinct_elements, 0);
            assert_eq!(meta.max_slots, ((meta.nslots as f64) * 0.95) as usize);
        }
    }
}

impl LogicalData {
    fn new(meta: &Metadata) -> LogicalData {
        LogicalData {
            physical: PhysicalData::new(meta.nslots, meta.rbits),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod logicaldata_tests {}

impl PhysicalData {
    fn new(nblocks: usize, rbits: usize) -> PhysicalData {
        // Allocate a vector of Block structures.  Because of how this structure will be used,
        // the contents should be populated in advance
        let blocks = vec![Default::default(); nblocks];
        PhysicalData { blocks, rbits }
    }
}

#[cfg(test)]
mod physical_data_tests {
    use super::*;

    #[test]
    fn creates_blocks() {
        let pd = PhysicalData::new(10, BITS_PER_SLOT);

        assert_eq!(pd.blocks.len(), 10);
        assert_eq!(pd.blocks[0], Default::default());
    }
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
    pub fn get_slot(&self, rbits: usize, relative_index: u64) -> u64 {
        let (slot_word_index, slot_bit_offset_in_word, bits_in_first_word, bits_in_second_word) =
            self.find_slot(rbits, relative_index);
        let rbits = rbits as u64;

        //Each block contains SLOTS_PER_BLOCK slots where each slot is rbits bits long.  The slots
        //are stored in an array of u64 words.  If rbits is an integer divisor of SLOTS_PER_BLOCK
        //then slots cannot span multiple words, but rbits is a runtime parameter so that can't be
        //assumed.
        //
        //Therefore when attempting to read a slot value there are two possible cases:
        //* slot is entirely within a single word
        //* slot starts on one word and ends on another
        if bits_in_second_word == 0 {
            //All of the slot is in one word; this statistically is the more common case
            (self.slots[slot_word_index as usize] >> slot_bit_offset_in_word) & bitmask!(rbits)
        } else {
            //This slot starts on one word and ends on the following word so extract both pieces
            //then put them together
            //If a slot spans two words, we know the the part of the slot's value in the second
            //word starts at bit 0 in that word, so unlike for the first part which requires a bit
            //shift and then a bit mask, the second part can be obtained with just a bit mask
            let first_part = (self.slots[slot_word_index as usize] >> slot_bit_offset_in_word)
                & bitmask!(bits_in_first_word);
            let second_part =
                self.slots[slot_word_index as usize + 1] & bitmask!(bits_in_second_word);

            first_part | (second_part << bits_in_first_word)
        }
    }

    /// Sets the value of the slot at `relative_index` to `val`.  Note the type of `val` is `u64`
    /// but its the `rbits` parameter that determines how many bits are actually stored in the slot
    #[inline]
    #[allow(dead_code)] // Just for now until higher level modules call this
    pub fn set_slot(&mut self, rbits: usize, relative_index: u64, val: u64) -> () {
        let (slot_word_index, slot_bit_offset_in_word, bits_in_first_word, bits_in_second_word) =
            self.find_slot(rbits, relative_index);

        println!(
            "index {:x}, offset {:x}, bits ({}, {})",
            slot_word_index, slot_bit_offset_in_word, bits_in_first_word, bits_in_second_word
        );
        if bits_in_second_word == 0 {
            //This slot is entirely contained within one word
            let old_word = self.slots[slot_word_index as usize];
            let slot_bitmask = bitmask!(rbits) << slot_bit_offset_in_word;
            let new_word =
                (old_word & !slot_bitmask) | ((val << slot_bit_offset_in_word) & slot_bitmask);

            println!(
                "single word at 0x{:x} changing from {:08x} to {:08x}",
                slot_word_index, old_word, new_word
            );

            self.slots[slot_word_index as usize] = new_word;
        } else {
            //This slot starts on one word and ends on the following word so extract both pieces
            //and then put them in the two words

            //If a slot spans two words, we know the the part of the slot's value in the second
            //word starts at bit 0 in that word, so unlike for the first part which requires a bit
            //shift and then a bit mask, the second part can be obtained with just a bit mask
            let old_word = self.slots[slot_word_index as usize];
            let slot_bitmask = bitmask!(bits_in_first_word) << slot_bit_offset_in_word;
            let new_word =
                (old_word & !slot_bitmask) | ((val << slot_bit_offset_in_word) & slot_bitmask);

            println!(
                "first word at 0x{:x} changing from {:08x} to {:08x}",
                slot_word_index, old_word, new_word
            );
            self.slots[slot_word_index as usize] = new_word;

            let old_word = self.slots[slot_word_index as usize + 1];
            let slot_bitmask = bitmask!(bits_in_second_word);
            let new_word =
                (old_word & !slot_bitmask) | ((val >> bits_in_first_word) & slot_bitmask);
            println!(
                "second word at 0x{:x} changing from {:08x} to {:08x}",
                slot_word_index + 1,
                old_word,
                new_word
            );
            self.slots[slot_word_index as usize + 1] = new_word;
        }
    }

    /// Given a relative index of a slot and the `rbits` number of bits per slot, finds the
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
    fn find_slot(&self, rbits: usize, relative_index: u64) -> (u64, u64, u64, u64) {
        let slots_per_block = SLOTS_PER_BLOCK as u64;
        let block_word_size = BLOCK_WORD_SIZE as u64;
        assert!(relative_index < slots_per_block);

        //Each block contains SLOTS_PER_BLOCK slots where each slot is rbits bits long.  The slots
        //are stored in an array of u64 words.  If rbits is an integer divisor of SLOTS_PER_BLOCK
        //then slots cannot span multiple words, but rbits is a runtime parameter so that can't be
        //assumed.
        //
        //Therefore when attempting to read a slot value there are two possible cases:
        //* slot is entirely within a single word
        //* slot starts on one word and ends on another
        let rbits = rbits as u64;
        let slot_word_index = relative_index * rbits / block_word_size;
        let slot_bit_offset_in_word = relative_index * rbits % block_word_size;

        if slot_bit_offset_in_word + rbits <= block_word_size {
            //This slot is entirely contained within one word
            (slot_word_index, slot_bit_offset_in_word, rbits, 0)
        } else {
            //This slot starts on one word and ends on the following word so extract both pieces
            //then put them together

            let bits_in_first_word = block_word_size - slot_bit_offset_in_word;
            let bits_in_second_word = rbits - bits_in_first_word;

            (
                slot_word_index,
                slot_bit_offset_in_word,
                bits_in_first_word,
                bits_in_second_word,
            )
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
            assert_eq!(0, block.get_slot(BITS_PER_SLOT, i as u64));
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
    pub fn get_set_block_tests() {
        //generate an array of random u64 values, one for each possible slot in the block
        //iterate over them, calling get_slot and confirming 0, set_slot, then get_slot again
        //confirming the correct value
        //
        //After all values are obtained, run through again, this time calling get_slot to confirm
        //the expected value, then set_slot to set a 0 value, and get_slot to confirm the 0 value
        let mut random_values: Vec<u64> = Vec::with_capacity(SLOTS_PER_BLOCK);
        for _ in 0..SLOTS_PER_BLOCK {
            random_values.push(rand::random::<u64>());
        }

        let mut block = Block {
            ..Default::default()
        };

        let rbitmask = bitmask!(BITS_PER_SLOT);

        for i in 0..SLOTS_PER_BLOCK {
            assert_eq!(0, block.get_slot(BITS_PER_SLOT, i as u64));

            let r = random_values[i] & rbitmask;

            //Set the value of r to the slot.  Note that we don't use the masked-out r we use the
            //full 64-bit random value.  This verifies that set_slot masks out the unused bits
            block.set_slot(BITS_PER_SLOT, i as u64, random_values[i]);

            println!("After setting slot {} to value {:x}, block slots:", i, r);
            for j in 0..BITS_PER_SLOT {
                println!("Word {}: {:08x}", j, block.slots[j]);
            }

            assert_eq!(r, block.get_slot(BITS_PER_SLOT, i as u64));
        }

        for i in 0..SLOTS_PER_BLOCK {
            let r = random_values[i] & rbitmask;
            assert_eq!(r, block.get_slot(BITS_PER_SLOT, i as u64));

            //Set this slot back to 0 and make sure that stuck
            block.set_slot(BITS_PER_SLOT, i as u64, 0);

            assert_eq!(0, block.get_slot(BITS_PER_SLOT, i as u64));
        }
    }

    #[test]
    pub fn find_slot_tests() {
        let bits_per_slot = BITS_PER_SLOT as u64;
        let mut expected_word_index = 0u64;
        let mut expected_bit_offset_in_word = 0u64;
        let mut expected_bits_in_first_word = bits_per_slot;
        let mut expected_bits_in_second_word = 0u64;

        let block = Block {
            ..Default::default()
        };

        for i in 0..SLOTS_PER_BLOCK {
            let (slot_word_index, slot_bit_offset_in_word, bits_in_first_word, bits_in_second_word) =
                block.find_slot(BITS_PER_SLOT, i as u64);

            assert_eq!(expected_word_index, slot_word_index);
            assert_eq!(expected_bit_offset_in_word, slot_bit_offset_in_word);
            assert_eq!(expected_bits_in_first_word, bits_in_first_word);
            assert_eq!(expected_bits_in_second_word, bits_in_second_word);

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
