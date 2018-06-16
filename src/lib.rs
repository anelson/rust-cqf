const BITS_PER_SLOT: usize = 8;
const BLOCK_OFFSET_BITS: usize = (6); //6 seems fastest
const SLOTS_PER_BLOCK: usize = (1 << BLOCK_OFFSET_BITS);
const METADATA_WORDS_PER_BLOCK: usize = ((SLOTS_PER_BLOCK + 63) / 64);

struct Block {
    offset: u8,
    occupieds: [u64; METADATA_WORDS_PER_BLOCK],
    runends: [u64; METADATA_WORDS_PER_BLOCK],
    slots: [u8; SLOTS_PER_BLOCK],
}

pub struct CQF {}
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
