use block;
use murmur::Murmur3Hash;
use physical;

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
    physical: physical::PhysicalData,
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
        assert!(block::SLOTS_PER_BLOCK == 64usize); //this code assumes 64 slots per block always
        assert!(rbits as usize == block::BITS_PER_SLOT); //TODO: figure out how to make this configurable

        let qbits = Metadata::calculate_qbits(n, rbits);
        let total_slots = 1usize << qbits; //2^qbits slots in the filter
        let nblocks = (total_slots + block::SLOTS_PER_BLOCK - 1) / block::SLOTS_PER_BLOCK;

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
            physical: physical::PhysicalData::new(meta.nslots, meta.rbits),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod logicaldata_tests {}
