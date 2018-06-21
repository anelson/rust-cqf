use murmur::Murmur3Hash;
use std::vec::Vec;

const BITS_PER_SLOT: usize = 9; //corresponds to false positive rate of 1/(2^9) == 1/512
const BLOCK_OFFSET_BITS: usize = (6); //6 seems fastest; corresponds to max offset 2^6-1 or 63
const SLOTS_PER_BLOCK: usize = (1 << BLOCK_OFFSET_BITS); //64 currently
const METADATA_WORDS_PER_BLOCK: usize = ((SLOTS_PER_BLOCK + 63) / 64); //64 bits per word

#[allow(dead_code)] // for now
#[derive(Default)]
struct Block {
    offset: u8,
    occupieds: [u64; METADATA_WORDS_PER_BLOCK],
    runends: [u64; METADATA_WORDS_PER_BLOCK],
    slots: [u64; BITS_PER_SLOT],
}

#[allow(dead_code)] // for now
#[derive(Default)]
struct Metadata {
    n: usize,
    qbits: u8,
    rbits: u8,
    nblocks: usize,
    nelements: usize,
    ndistinct_elements: usize,
    nslots: usize,
    noccupied_slots: usize,
    max_slots: usize,
}

#[allow(dead_code)] // for now
pub struct RSQF {
    meta: Metadata,
    blocks: Vec<Block>,
}

/// Standard filter result type, on success returns a count on error returns a message
/// This should probably be richer over time
type FilterResult = Result<usize, &'static str>;

#[allow(dead_code)] // for now
#[allow(unused_variables)] // for now
impl RSQF {
    pub fn new(n: usize, rbits: u8) -> RSQF {
        assert!(SLOTS_PER_BLOCK == 64usize); //this code assumes 64 slots per block always
        assert!(rbits as usize == BITS_PER_SLOT); //TODO: figure out how to make this configurable
        let meta = RSQF::calculate_metadata(n, rbits);
        let blocks = Vec::with_capacity(meta.nblocks);
        return RSQF { meta, blocks };
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

    /// Given the insert count `n` and the remainder bits `rbits`, computes the metadata
    /// for a filter which will have worst-case false positive rate of `2^-rbits`
    fn calculate_metadata(n: usize, rbits: u8) -> Metadata {
        assert!(rbits > 1);
        assert!(n > 0);

        let sigma = 2.0f64.powi(-(rbits as i32));
        let p = ((n as f64) / sigma).log2().ceil() as u8;

        assert!(p > rbits);

        let qbits = p - rbits;

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

    fn get_q_and_r(&self, hash: Murmur3Hash) -> (u64, u64) {
        //Use only the 64-bit hash and pull out the bits we'll use for q and r
        let hash = hash.value64();

        // To compute the quotient q for this hash, shift right to remove the bits to be used as
        // the remainder r, then mask out q bits
        let q = (hash.wrapping_shr(self.meta.qbits as u32)) & bitmask!(self.meta.qbits);
        let r = hash & bitmask!(self.meta.rbits as u32);

        (q, r)
    }
}

#[cfg(test)]
mod tests {
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
            // (qbits, rbits, hash, expected_q, expected_r)
            (30u8, 9u8, 0x0000_0000u128, 0u64, 0u64),
        ];

        for (qbits, rbits, _hash, _expected_q, _expected_r) in test_data.into_iter() {
            let _meta = Metadata {
                qbits: *qbits,
                rbits: *rbits,
                ..Default::default()
            };
        }
    }
}
