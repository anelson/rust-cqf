use std::vec::Vec;

const BITS_PER_SLOT: usize = 9; //corresponds to false positive rate of 1/512
const BLOCK_OFFSET_BITS: usize = (6); //6 seems fastest
const SLOTS_PER_BLOCK: usize = (1 << BLOCK_OFFSET_BITS);
const METADATA_WORDS_PER_BLOCK: usize = ((SLOTS_PER_BLOCK + 63) / 64);

struct Block {
    offset: u8,
    occupieds: [u64; METADATA_WORDS_PER_BLOCK],
    runends: [u64; METADATA_WORDS_PER_BLOCK],
    slots: [u64; BITS_PER_SLOT],
}

struct Metadata {
    n: usize,
    q: u8,
    r: u8,
    nblocks: usize,
    nelements: usize,
    ndistinct_elements: usize,
    nslots: usize,
    noccupied_slots: usize,
    max_slots: usize,
}

pub struct RSQF {
    meta: Metadata,
    blocks: Vec<Block>,
}

/// The type which is used as the key in this structure.  The code is written and optimized for
/// a 64-bit hash.  Any other type could be used but the caller is responsible for implementing
/// a consistent hash function which produces 64-bit outputs.  MurmurHash is a good choice.
type HashKey = u64;

/// Standard filter result type, on success returns a count on error returns a message
/// This should probably be richer over time
type FilterResult = Result<usize, &'static str>;

impl RSQF {
    pub fn new(n: usize, r: u8) -> RSQF {
        assert!(SLOTS_PER_BLOCK == 64usize); //this code assumes 64 slots per block always
        assert!(r as usize == BITS_PER_SLOT); //TODO: figure out how to make this configurable
        let meta = RSQF::calculate_metadata(n, r);
        let blocks = Vec::with_capacity(meta.nblocks);
        return RSQF { meta, blocks };
    }

    /// Queries the filter for the presence of `hash`.
    ///
    /// If `hash` is not present, returns 0
    /// If `hash` is likely to be present, returns an approximate count of the number of times
    /// `hash` has been inserted.  Note that this is approximate; it is possible that `hash` is
    /// actually not present but a non-zero count is returned, with a probability no worse than
    /// `2^-r`
    pub fn get_count(&self, hash: HashKey) -> usize {
        panic!("NYI");
        return 0;
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
    pub fn add_count(&self, hash: HashKey, count: usize) -> FilterResult {
        panic!("NYI");
        return Ok(0);
    }

    /// Increments the count of `hash` by one.
    ///
    /// If `hash` is not present, it's added with a count of one.
    /// If `hash` is present, its existing count is incremented.
    ///
    /// Returns the new total count of `hash` on success, or `Err` if the filter is already at max
    /// capacity
    pub fn inc_count(&self, hash: HashKey) -> FilterResult {
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
    pub fn sub_count(&self, hash: HashKey, count: usize) -> FilterResult {
        panic!("NYI");
        return Ok(0);
    }

    pub fn dec_count(&self, hash: HashKey) -> FilterResult {
        return self.sub_count(hash, 1);
    }

    /// Given the insert count `n` and the remainder bits `r`, computes the metadata
    /// for a filter which will have worst-case false positive rate of `2^-r`
    fn calculate_metadata(n: usize, r: u8) -> Metadata {
        assert!(r > 1);
        assert!(n > 0);

        let sigma = 2.0f64.powi(-(r as i32));
        let p = ((n as f64) / sigma).log2().ceil() as u8;

        assert!(p > r);

        let q = p - r;

        let total_slots = 1usize << q; //2^q slots in the filter
        let nblocks = (total_slots + SLOTS_PER_BLOCK - 1) / SLOTS_PER_BLOCK;

        //Conservatively, set the maximum number of elements to 95% of the total capacity
        //Realistically this structure can go higher than that but there starts to be a performance
        //penalty and it's better to resize at that point
        let max_slots = ((total_slots as f64) * 0.95) as usize;

        return Metadata {
            n,
            r,
            q,
            nblocks,
            nelements: 0,
            ndistinct_elements: 0,
            nslots: total_slots,
            noccupied_slots: 0,
            max_slots,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::RSQF;

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
        assert_eq!(filter.meta.r, 9);
        assert_eq!(filter.meta.q, 14);
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
    fn get_count_nonexistent_item_returns_zero() {
        let filter = RSQF::new(10000, 9);

        assert_eq!(0, filter.get_count(1));
    }
}
