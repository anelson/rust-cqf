//! Contains routines that perform various kinds of bit fiddling operations used by RSQF
//! Using the power of Rust intrinsics, will use advanced SIMD instructions where available
//!
//! TODO: Currently implemented only in terms of x86_64 intrinsics.  Still need to implement
//! fall-back in processor-neutral Rust and possibly other processor-specific implementations

#![macro_use]

/// Internal macro which generates a compile-time expression that evaluates to a u64 bitmask with
/// the lower `n` bits set to 1, where `n` is the parameter to the macro
///
/// # Examples
///
/// ```
/// # #[macro_use] extern crate cqf;
/// # fn main() {
///
/// assert_eq!(0, bitmask!(0));
/// assert_eq!(0xffff_ffff_ffff_ffff_u64, bitmask!(64));
/// assert_eq!(0xffff_ffff_u64, bitmask!(32));
/// assert_eq!(0b011111, bitmask!(5));
/// # }
/// ```
#[macro_export]
macro_rules! bitmask {
    ($bits:expr) => {{
        assert!($bits <= 64);

        if $bits == 64 {
            0xffff_ffff_ffff_ffff_u64
        } else {
            1_u64.wrapping_shl(($bits) as u32) - 1_u64
        }
    }};
}

// If this is an x86-64 CPU target bring in the x86-64 intrinsics
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// A trait which brings in various bit-fiddling methods intended to be used with unsigned integer
/// types.
pub trait BitFiddling {
    /// Given a 64-bit integer, returns the number of bits set to 1
    #[inline]
    fn popcnt(self) -> usize;

    /// Computes the `popcnt` bit count operation, but skips the first `skip_bits` bits in the selfue
    /// before starting the count.  If `skip_bits` is 0, this is equiselfent to `popcnt`
    #[inline]
    fn popcnt_skip_n(self, skip_bits: usize) -> usize;

    /// Counts the number of set bits up to a certain bit position
    /// Like the opposite of `popcnt_skip_n`, except `end_bit` is INCLUSIVE, so if `end_bit` is 0 then
    /// this tests only the first bit, whereas `popcnt_skip_n` with a `skip_bits` of 0 will not skip
    #[inline]
    fn popcnt_first_n(self, end_bit: usize) -> usize;

    /// Performs a reverse bit scan, finding the index of the highest set bit.
    ///
    /// #Returns
    ///
    /// `None` if the selfue has no set bits (that is, if `self` is 0), or the 0-based index of the
    /// highest set bit
    #[inline]
    fn bit_scan_reverse(self) -> Option<usize>;

    /// Finds the index of the lowest set bit in the selfue
    ///
    /// #Returns
    ///
    /// `None` if the selfue has no set bits (that is, if `self` is 0), or the 0-based index of the
    /// lowest set bit
    #[inline]
    fn bit_scan_forward(self) -> Option<usize>;

    /// Returns the position of the rank'th 1.  (rank = 0 returns the 1st 1)
    /// Returns None if there are fewer than rank+1 1s.
    #[inline]
    fn bitselect(self, rank: usize) -> Option<usize>;

    /// Finds the position of the rank-th bit in a word, skipping `skip_bits` bits first.
    /// If `skip_bits` is 0, this is equiselfent to `bitselect`
    /// Returns None if there are fewer than rank+1 1s after `skip_bits`.
    #[inline]
    fn bitselect_skip_n(self, rank: usize, skip_bits: usize) -> Option<usize>;
}

impl BitFiddling for u64 {
    #[inline]
    fn popcnt(self) -> usize {
        //popcnt_x86_64(self)
        self.count_ones() as usize
    }

    #[inline]
    fn popcnt_skip_n(self, skip_bits: usize) -> usize {
        assert!(skip_bits <= 64);
        if skip_bits < 64 {
            //Simply mask out the first skip_bits bits
            let mask = !bitmask!(skip_bits);
            (self & mask).popcnt()
        } else {
            //If this 64-bit integer has any bits set after the first 64, it's a most unusual integer
            //indeed
            0
        }
    }

    #[inline]
    fn popcnt_first_n(self, end_bit: usize) -> usize {
        assert!(end_bit <= 64);
        //Pretty easy, just mask self so only the first end_bit bits (inclusive)
        //are set
        if end_bit < 63 {
            let mask = bitmask!(end_bit + 1);
            (self & mask).popcnt()
        } else {
            self.popcnt()
        }
    }

    #[inline]
    fn bit_scan_reverse(self) -> Option<usize> {
        if self != 0 {
            Some(63 - self.leading_zeros() as usize)
        } else {
            None
        }
    }

    #[inline]
    fn bit_scan_forward(self) -> Option<usize> {
        if self != 0 {
            Some(self.trailing_zeros() as usize)
        } else {
            None
        }
    }

    #[inline]
    fn bitselect(self, rank: usize) -> Option<usize> {
        #[cfg(all(target_arch = "x86_64", target_feature = "bmi2", target_feature = "bmi1"))]
        {
            let pos = bitselect_x86_64_bmi2(self, rank);

            if pos != 64 {
                Some(pos as usize)
            } else {
                None
            }
        }

        #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2", target_feature = "bmi1")))]
        panic!("Only implemented for current generation x86_64 processors!")
    }

    #[inline]
    fn bitselect_skip_n(self, rank: usize, skip_bits: usize) -> Option<usize> {
        (self & !bitmask!(skip_bits)).bitselect(rank)
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "bmi2", target_feature = "bmi1"))]
#[inline]
fn bitselect_x86_64_bmi2(val: u64, rank: usize) -> u64 {
    unsafe {
        // This is a novel (to me) use of the pdep instruction
        // The 'mask' parameter to pdep is actually the selfue we're interested in
        // The 'selfue' is a mask with a '1' bit in the rank-th position.
        //
        // We run the pdep instruction, then use tzcnt to count the leading zeros which tells us by
        // how many bits the input selfue was shifted and thus the rank of the rank-th bit.
        let mask = 1u64 << rank;

        _tzcnt_u64(_pdep_u64(mask, val))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_bitmask() {
        assert_eq!(0, bitmask!(0));
        assert_eq!(0b01, bitmask!(1));
        assert_eq!(0b011, bitmask!(2));
        assert_eq!(0x7fff_ffff_ffff_ffff_u64, bitmask!(63));
        assert_eq!(0xffff_ffff_ffff_ffff_u64, bitmask!(64));
    }

    #[test]
    #[should_panic]
    fn test_bitmask_invalid_bit_count() {
        bitmask!(65);
    }

    #[test]
    fn test_popcnt() {
        assert_eq!(0, 0b000000.popcnt());
        assert_eq!(0, 0b000000.popcnt());
        assert_eq!(1, 0b000001.popcnt());
        assert_eq!(1, 0b100000.popcnt());
        assert_eq!(2, 0b100001.popcnt());
        assert_eq!(32, 0xffff_ffff_u64.popcnt());
        assert_eq!(64, 0xffff_ffff_ffff_ffff_u64.popcnt());

        let k = 1u64;

        for i in 0..64 {
            assert_eq!(1, (k << i).popcnt());
        }

        let k = 0xffff_ffff_ffff_ffff_u64;
        for i in 0..64 {
            assert_eq!(64 - i, k.wrapping_shr(i as u32).popcnt());
        }
    }

    #[test]
    fn test_popcnt_skip_n() {
        assert_eq!(0, 0b000000.popcnt_skip_n(0));
        assert_eq!(1, 0b000001.popcnt_skip_n(0));
        assert_eq!(0, 0b000001.popcnt_skip_n(1));
        assert_eq!(1, 0b100000.popcnt_skip_n(0));
        assert_eq!(1, 0b100000.popcnt_skip_n(1));
        assert_eq!(1, 0b100000.popcnt_skip_n(5));
        assert_eq!(0, 0b100000.popcnt_skip_n(6));
        assert_eq!(2, 0b100001.popcnt_skip_n(0));
        assert_eq!(1, 0b100001.popcnt_skip_n(1));
        assert_eq!(1, 0b100001.popcnt_skip_n(5));
        assert_eq!(0, 0b100001.popcnt_skip_n(6));
        assert_eq!(1, 0xffff_ffff_ffff_ffff_u64.popcnt_skip_n(63));
        assert_eq!(0, 0xffff_ffff_ffff_ffff_u64.popcnt_skip_n(64));

        let k = 0xffff_ffff_ffff_ffff_u64;

        for i in 0..64 {
            assert_eq!(64 - i, k.popcnt_skip_n(i));
        }
    }

    #[test]
    fn test_popcnt_first_n() {
        assert_eq!(0, 0b000000.popcnt_first_n(0));
        assert_eq!(1, 0b000001.popcnt_first_n(0));
        assert_eq!(1, 0b000001.popcnt_first_n(1));
        assert_eq!(0, 0b100000.popcnt_first_n(0));
        assert_eq!(0, 0b100000.popcnt_first_n(1));
        assert_eq!(1, 0b100000.popcnt_first_n(5));
        assert_eq!(1, 0b100000.popcnt_first_n(6));
        assert_eq!(1, 0b100001.popcnt_first_n(0));
        assert_eq!(1, 0b100001.popcnt_first_n(1));
        assert_eq!(1, 0b100001.popcnt_first_n(2));
        assert_eq!(1, 0b100001.popcnt_first_n(4));
        assert_eq!(2, 0b100001.popcnt_first_n(5));
        assert_eq!(2, 0b100001.popcnt_first_n(6));
        assert_eq!(64, 0xffff_ffff_ffff_ffff_u64.popcnt_first_n(63));
        assert_eq!(64, 0xffff_ffff_ffff_ffff_u64.popcnt_first_n(64));

        let k = 0xffff_ffff_ffff_ffff_u64;

        for i in 0..64 {
            assert_eq!(i + 1, k.popcnt_first_n(i));
        }
    }

    #[test]
    fn test_bit_scan_forward() {
        assert_eq!(None, 0.bit_scan_forward());
        assert_eq!(Some(0), 0x01.bit_scan_forward());
        assert_eq!(Some(1), (0x01_u64 << 1).bit_scan_forward());
        assert_eq!(Some(5), (0x01_u64 << 5).bit_scan_forward());
        assert_eq!(Some(9), (0x01_u64 << 9).bit_scan_forward());
        assert_eq!(Some(33), (0x01_u64 << 33).bit_scan_forward());
        assert_eq!(Some(63), (0x01_u64 << 63).bit_scan_forward());
    }

    #[test]
    fn test_bit_scan_reverse() {
        assert_eq!(Some(0), 0b01_u64.bit_scan_reverse());
        assert_eq!(Some(1), 0b11_u64.bit_scan_reverse());
        assert_eq!(Some(63), 0xffff_ffff_ffff_ffff_u64.bit_scan_reverse());
    }

    #[test]
    fn test_bitselect() {
        assert_eq!(None, 0x0.bitselect(0));
        assert_eq!(Some(1), 0b100010.bitselect(0));
        assert_eq!(Some(5), 0b100010.bitselect(1));
        assert_eq!(None, 0b100010.bitselect(2));
    }

    #[test]
    fn test_bitselect_skip_n() {
        assert_eq!(None, 0x0.bitselect_skip_n(0, 0));
        assert_eq!(Some(1), 0b100010.bitselect_skip_n(0, 0));
        assert_eq!(Some(1), 0b100010.bitselect_skip_n(0, 1));
        assert_eq!(Some(5), 0b100010.bitselect_skip_n(0, 2));
        assert_eq!(Some(5), 0b100010.bitselect_skip_n(1, 0));
        assert_eq!(None, 0b100010.bitselect_skip_n(2, 0));
        assert_eq!(None, 0b100010.bitselect_skip_n(1, 2));

        let val = 0b_1000_1111_0001u64;

        assert_eq!(Some(0), val.bitselect_skip_n(0, 0));
        assert_eq!(Some(4), val.bitselect_skip_n(0, 1));
        assert_eq!(None, val.bitselect_skip_n(0, 12));
    }
}
