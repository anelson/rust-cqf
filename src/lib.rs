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
}

pub struct CQF {
    meta: Metadata,
    blocks: Vec<Block>,
}

impl CQF {
    pub fn new(n: usize, r: u8) -> CQF {
        assert!(SLOTS_PER_BLOCK == 64usize); //this code assumes 64 slots per block always
        assert!(r as usize == BITS_PER_SLOT); //TODO: figure out how to make this configurable
        let meta = CQF::calculate_metadata(n, r);
        let blocks = Vec::with_capacity(meta.nblocks);
        return CQF { meta, blocks };
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

        return Metadata {
            n,
            r,
            q,
            nblocks,
            nelements: 0,
            ndistinct_elements: 0,
            nslots: total_slots,
            noccupied_slots: 0,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::CQF;

    #[test]
    fn creates_empty_filter() {
        let filter = CQF::new(10000, 9);
    }

    #[test]
    #[should_panic]
    fn panics_on_invalid_r() {
        CQF::new(10000, 8);
    }

    #[test]
    fn computes_valid_metadata() {
        let filter = CQF::new(10000, 9);

        assert_eq!(filter.meta.n, 10000);
        assert_eq!(filter.meta.r, 9);
        assert_eq!(filter.meta.q, 14);
        assert_eq!(filter.meta.nslots, 1usize << 14);
        assert_eq!(filter.meta.nblocks, (filter.meta.nslots + 64 - 1) / 64);
    }
}
