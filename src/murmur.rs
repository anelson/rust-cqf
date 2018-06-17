#[inline]
unsafe fn get_block64(p: *const u64, i: usize) -> u64 {
    return *p.offset(i as isize);
}

#[inline]
fn rotl(x: u64, k: u32) -> u64 {
    let k = k as u64;
    return (x << k) | (x >> (64 - k));
}

#[inline]
fn fmix64(k: u64) -> u64 {
    let mut k = k;

    k ^= k >> 33;
    k = k.wrapping_mul(0xff51afd7ed558ccd_u64);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ceb9fe1a85ec53_u64);
    k ^= k >> 33;

    return k;
}

#[inline]
unsafe fn read_u8_as_u64(data: *const u8) -> u64 {
    return (*data) as u64;
}

#[inline]
unsafe fn read_u16_as_u64(data: *const u8) -> u64 {
    return (*(data as *const u16)) as u64;
}

#[inline]
unsafe fn read_u32_as_u64(data: *const u8) -> u64 {
    return (*(data as *const u32)) as u64;
}

#[inline]
unsafe fn read_u64_as_u64(data: *const u8) -> u64 {
    return *(data as *const u64);
}

#[inline]
/// Given a byte array, tries to read a 64-bit unsigned integer out of it
/// It's expected that the buffer might not contain the full 8 bytes needed to construct a 64-bit
/// integer, in which case as much of the integer will be read from memory as possible, in little
/// endian byte order
unsafe fn read_partial_u64(data: *const u8, len: usize) -> u64 {
    match len {
        0 => 0u64,
        1 => read_u8_as_u64(data),
        2 => read_u16_as_u64(data),
        3 => read_u16_as_u64(data) | read_u8_as_u64(data.offset(2)) << 16,
        4 => read_u32_as_u64(data),
        5 => read_u32_as_u64(data) | read_u8_as_u64(data.offset(4)) << 32,
        6 => read_u32_as_u64(data) | read_u16_as_u64(data.offset(4)) << 32,
        7 => {
            read_u32_as_u64(data) | read_u16_as_u64(data.offset(4)) << 32
                | read_u8_as_u64(data.offset(6)) << 48
        }
        _ => read_u64_as_u64(data),
    }
}

pub fn unsafe_murmur_hash3(key: *const u8, len: usize, seed: u32) -> u128 {
    const C1: u64 = 0x87c37b91114253d5_u64;
    const C2: u64 = 0x4cf5ad432745937f_u64;

    unsafe {
        let data: *const u8 = key;
        let nblocks = len / 16; //each block is 128 bits

        let mut h1 = seed as u64;
        let mut h2 = seed as u64;

        // body
        let blocks: *const u64 = data as *const u64;
        for i in 0..nblocks {
            let mut k1 = get_block64(blocks, i * 2 + 0);
            let mut k2 = get_block64(blocks, i * 2 + 1);

            k1 = k1.wrapping_mul(C1);
            k1 = rotl(k1, 31);
            k1 = k1.wrapping_mul(C2);
            h1 ^= k1;

            h1 = rotl(h1, 27);
            h1.wrapping_add(h2);
            h1 = h1.wrapping_mul(5).wrapping_add(0x52dce729);

            k2 = k2.wrapping_mul(C2);
            k2 = rotl(k2, 33);
            k2 = k2.wrapping_mul(C1);
            h2 ^= k2;

            h2 = rotl(h2, 31);
            h2.wrapping_add(h1);
            h2 = h2.wrapping_mul(5).wrapping_add(0x38495ab5);
        }

        //tail
        let tail: *const u8 = data.offset(nblocks as isize * 16);
        let mut k1 = 0_u64;
        let mut k2 = 0_u64;

        //these are the bytes left over after zero or more 16-byte chunks are processed above
        //the intention here is to XOR these into k1 and k2 using similar logic as above, with
        //whatever fraction of a full 16-byte block remains
        //
        //The C code uses a big switch statement and takes advantage of the C/C++ switch/case
        //gotcha whereby case statements fall through to the next case statement.  That construct
        //doesn't have an equivalent in Rust (thank Christ!) so we'll take a different approach
        let remaining = len & 0x0f; //equivalent to len % 16
        if remaining > 0 {
            //There's at least 1 and possibly 8 bytes for processing in k1
            let x = read_partial_u64(tail, remaining);
            k1 ^= x;

            k1 = k1.wrapping_mul(C1);
            k1 = rotl(k1, 31);
            k1 = k1.wrapping_mul(C2);
            h1 ^= k1;

            if remaining > 8 {
                //There's at least one and possibly up to 7 bytes for processing in k2
                let x = read_partial_u64(tail.offset(8), remaining - 8);
                k2 ^= x;

                k2 = k2.wrapping_mul(C2);
                k2 = rotl(k2, 33);
                k2 = k2.wrapping_mul(C1);
                h2 ^= k2;
            }
        }

        // finalization
        h1 ^= len as u64;
        h2 ^= len as u64;

        h1 = h1.wrapping_add(h2);
        h2 = h2.wrapping_add(h1);

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 = h1.wrapping_add(h2);
        h2 = h2.wrapping_add(h1);

        return (h2 as u128) << 64 | (h1 as u128);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_partial_u64_all_variations() {
        let data: [u8; 10] = [0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99];
        let data_ptr = data.as_ptr();

        unsafe {
            assert_eq!(read_partial_u64(data_ptr, 0), 0);
            assert_eq!(read_partial_u64(data_ptr, 1), 0x00);
            assert_eq!(read_partial_u64(data_ptr, 2), 0x1100);
            assert_eq!(read_partial_u64(data_ptr, 3), 0x221100);
            assert_eq!(read_partial_u64(data_ptr, 4), 0x33221100);
            assert_eq!(read_partial_u64(data_ptr, 5), 0x4433221100);
            assert_eq!(read_partial_u64(data_ptr, 6), 0x554433221100);
            assert_eq!(read_partial_u64(data_ptr, 7), 0x66554433221100);
            assert_eq!(read_partial_u64(data_ptr, 8), 0x7766554433221100);
            assert_eq!(read_partial_u64(data_ptr, 9), 0x7766554433221100);
        }
    }

    #[test]
    fn unsafe_version_matches_c_impl() {
        //These test vectors came from the C implementation which it itself a port of the original
        //C++ implmeentation.  Hopefully they're equivalent
        //https://github.com/PeterScott/murmur3/blob/master/test.c
        struct TestVector {
            seed: u32,
            data: &'static [u8],
            hash: &'static str,
        }

        /// The murmur3.c test suite uses a strange text representation of the hash so we need to
        /// do the same
        fn hex_string(hash: u128) -> String {
            let hash0: u32 = hash as u32;
            let hash1: u32 = (hash >> 32) as u32;
            let hash2: u32 = (hash >> 64) as u32;
            let hash3: u32 = (hash >> 96) as u32;

            return format!("{:08x}{:08x}{:08x}{:08x}", hash0, hash1, hash2, hash3);
        }

        let test_vectors: Vec<TestVector> = vec![TestVector {
            seed: 123,
            data: b"",
            hash: "4cd9597081679d1abd92f8784bace33d",
        }];

        for test in test_vectors.iter() {
            let data = test.data.as_ptr();
            assert_eq!(
                test.hash,
                hex_string(unsafe_murmur_hash3(data, test.data.len(), test.seed)),
                "test failed for data with length {}, seed {}",
                test.data.len(),
                test.seed
            );
        }
    }

    #[test]
    fn uses_specialized_version() {
        //assert_eq!(1, 5u64.murmur_hash64(0));
    }
}
