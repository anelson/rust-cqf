//! Contains routines that perform various kinds of bit fiddling operations used by RSQF
//! Using the power of Rust intrinsics, will use advanced SIMD instructions where available
//!
//! TODO: Currently implemented only in terms of x86_64 intrinsics.  Still need to implement
//! fall-back in processor-neutral Rust and possibly other processor-specific implementations

#![macro_use]

use std::mem;

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
pub trait BitFiddling: Sized {
    fn bits_per_word() -> usize {
        mem::size_of::<Self>() * 8
    }

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
    fn bitselect_skip_n(self, skip_bits: usize, rank: usize) -> Option<usize>;

    /// Shifts the word left by `shift_bits` bits, inserting the lowest `shift_bits` bits of
    /// `shift_value` into the lowest bits of the word.  
    ///
    /// # Returns
    ///
    /// Returns a tuple containing two elements:
    ///
    /// * `result` - The new value of this word after applying the shift operation
    /// * `shifted_value` - The top `shift_bits` of `self` which were pushed out of the word by the
    /// shift operation.  Note that these bits are not returned in their original position, they
    /// are returned in another word having been shifted right so they start at bit 0.
    #[inline]
    fn shift_left_into(self, shift_bits: usize, shift_value: Self) -> (Self, Self)
    where
        Self: Sized;

    /// Specialized version of `shift_info` that operates on only a subset of bits in the word.
    ///
    /// `shift_left_into()` is equivalent to `shift_left_into_partial` with `start_bit` equal to `0` and
    /// `count_bits` equal to the number of bits in the word.
    ///
    /// # Arguments
    ///
    /// `start_bit` - The 0-based index in this word of the bit at which to start the shift
    /// operation.  Any bits prior to this are unmodified by the shift.
    /// `count_bits` - The number of bits to include in the shift operation.  Must be an integer
    /// multiple of `shift_bits`.  Any bits in the word after `start_bit+count_bits` are unmodified
    /// by the shift.
    /// `shift_bits` - The number of bits by which to shift the word's contents bitwise left
    /// `shift_value` - The `shift_bits`-bit value which will be placed in the region of the word
    /// which was vacated by the shfit operation.  That region will start at `start_bit`
    ///
    /// # Remarks
    ///
    /// This method is designed to support cases in which the shift operation shifts beyond the end
    /// of the word.  If the range being shifted goes beyond the end of this word, then the
    /// `shifted_value` value will not be accurate.  To make this accurate would require performing
    /// the shift using a larger word type and that is slower and not worth the effort for our
    /// application.
    ///
    /// # Returns
    ///
    /// Returns a tuple containing two elements:
    ///
    /// * `result` - The new value of this word after applying the shift operation
    /// * `shifted_value` - The top `shift_bits` of `self` which were pushed out of the word by the
    /// shift operation.  Note that these bits are not returned in their original position, they
    /// are returned in another word having been shifted right so they start at bit 0.
    #[inline]
    fn shift_left_into_partial(
        self,
        start_bit: usize,
        count_bits: usize,
        shift_bits: usize,
        shift_value: Self,
    ) -> (Self, Self)
    where
        Self: Sized;
}

impl BitFiddling for u64 {
    #[inline]
    fn popcnt(self) -> usize {
        //popcnt_x86_64(self)
        self.count_ones() as usize
    }

    #[inline]
    fn popcnt_skip_n(self, skip_bits: usize) -> usize {
        debug_assert!(skip_bits <= 64);
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
        debug_assert!(end_bit <= 64);
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
    fn bitselect_skip_n(self, skip_bits: usize, rank: usize) -> Option<usize> {
        (self & !bitmask!(skip_bits)).bitselect(rank)
    }

    #[inline]
    #[allow(unused_variables)]
    fn shift_left_into(self, shift_bits: usize, shift_value: Self) -> (Self, Self) {
        debug_assert!((shift_value & !bitmask!(shift_bits)) == 0);

        //This simpler variant of shift_left_into is easy because it shifts the entire word
        let new_word = (self << shift_bits) | shift_value;
        let shifted_value = self >> 64 - shift_bits;

        (new_word, shifted_value)
    }

    #[inline]
    fn shift_left_into_partial(
        self,
        start_bit: usize,
        count_bits: usize,
        shift_bits: usize,
        shift_value: Self,
    ) -> (Self, Self) {
        debug_assert!((shift_value & !bitmask!(shift_bits)) == 0);
        debug_assert!(count_bits > 0);
        //This is much more complicated because it's operating on a subset of the whole word.
        //First let's mask out just the subset that we're operating on:
        let mask = bitmask!(count_bits) << start_bit;
        let working_word = self & mask;
        let unmodified_word = self & !mask;

        let new_word =
            unmodified_word | (working_word << shift_bits & mask) | (shift_value << start_bit);

        //NB: if start_bit + count_bits is beyond the end of this word, it's possible this
        //operation will try to shift more than 64 bits.  That's why `wrapping_shr` is used
        let shifted_value = if start_bit + count_bits - shift_bits < Self::bits_per_word() {
            (working_word >> (start_bit + count_bits - shift_bits)) & bitmask!(shift_bits)
        } else {
            0
        };

        (new_word, shifted_value)
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
        assert_eq!(Some(1), 0b100010.bitselect_skip_n(1, 0));
        assert_eq!(Some(5), 0b100010.bitselect_skip_n(2, 0));
        assert_eq!(Some(5), 0b100010.bitselect_skip_n(0, 1));
        assert_eq!(None, 0b100010.bitselect_skip_n(0, 2));
        assert_eq!(None, 0b100010.bitselect_skip_n(2, 1));

        let val = 0b_1000_1111_0001u64;

        assert_eq!(Some(0), val.bitselect_skip_n(0, 0));
        assert_eq!(Some(4), val.bitselect_skip_n(1, 0));
        assert_eq!(None, val.bitselect_skip_n(12, 0));
    }

    #[test]
    fn shift_left_into_test() {
        assert_eq!((0, 0), 0x0.shift_left_into(9, 0));
        assert_eq!(
            (0xffee_eedd_ddcc_cc00, 0),
            0x00ff_eeee_dddd_cccc.shift_left_into(8, 0)
        );
        assert_eq!(
            (0xffee_eedd_ddcc_cc00, 0xff),
            0xffff_eeee_dddd_cccc.shift_left_into(8, 0)
        );
        assert_eq!(
            (0xfffe_eeed_dddc_ccca, 0x0f),
            0xffff_eeee_dddd_cccc.shift_left_into(4, 0x0a)
        );
    }

    #[test]
    fn shift_left_into_partial_test() {
        //THis is a very fussy method.  Testing it might seem tricky.
        const START_BIT: usize = 8;
        const COUNT_BITS: usize = 48;
        const TEST_WORD: u64 = 0xffff_eeee_dddd_cccc;

        assert_eq!(
            (0, 0),
            0x0.shift_left_into_partial(START_BIT, COUNT_BITS, 8, 0)
        );

        //when start_bits is 0 and count_bits is 64 this is the same as shift_info
        assert_eq!(
            (0xffee_eedd_ddcc_cc00, 0xff),
            TEST_WORD.shift_left_into_partial(0, 64, 8, 0)
        );

        assert_eq!(
            (0xffee_eedd_ddcc_00cc, 0xff),
            TEST_WORD.shift_left_into_partial(START_BIT, COUNT_BITS, 8, 0)
        );
        assert_eq!(
            (0xffee_dddd_ccaa_aacc, 0xffee),
            TEST_WORD.shift_left_into_partial(START_BIT, COUNT_BITS, 16, 0xaaaa)
        );

        //this is a test case from bitarray which is failing due to a shifting bug
        //shift in the 8 bits 0x55 starting at bit 0, shifting the first three bytes of the word
        const ANOTHER_TEST_WORD: u64 = 0xffee_ddcc_bbaa_9900;
        assert_eq!(
            (0xffee_ddcc_bb99_0055, 0xaa),
            ANOTHER_TEST_WORD.shift_left_into_partial(0, 8 * 3, 8, 0x55)
        );

        //this is an edge case that happens when operating on the boundaries of a word.
        //the shift operation will extend well past the word; the expected result is as if the word
        //had more bits after its actual end, and after the operation only the lower 64 bits are
        //returned.
        assert_eq!(
            (0x5fff_eeee_dddd_cccc, 0x00), //technical 0x0f was shifted out but when shifting off the end we don't care
            TEST_WORD.shift_left_into_partial(60, 16, 8, 0x55)
        );

        //this is another edge case, in which the count of bits to shift is not an integer multiple
        //of the shift_bits because it's up against the top end of the word
        assert_eq!(
            (0x5fff_eeee_dddd_cccc, 0xf0),
            TEST_WORD.shift_left_into_partial(60, 4, 8, 0x55)
        );
    }
}
