//! Contains routines that perform various kinds of bit fiddling operations used by RSQF
//! Using the power of Rust intrinsics, will use advanced SIMD instructions where available
//!
//! TODO: Currently implemented only in terms of x86_64 intrinsics.  Still need to implement
//! fall-back in processor-neutral Rust and possibly other processor-specific implementations

// If this is an x86-64 CPU target bring in the x86-64 intrinsics
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// Internal macro which generates a compile-time expression that evaluates to a u64 bitmask with
/// the lower `n` bits set to 1, where `n` is the parameter to the macro
///
/// # Examples
///
/// ```
/// assert_eq!(0, bitmask!(0));
/// assert_eq!(0xffff_ffff_ffff_ffff_u64, bitmask!(64));
/// assert_eq!(0xffff_ffff_u64, bitmask!(32));
/// assert_eq!(0b011111, bitmask!(5));
/// ```
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

/// Given a 64-bit integer, returns the number of bits set to 1
///
/// # Examples
///
/// ```
/// extern crate cqf;
/// use cqf::bitfiddling::popcnt;
///
/// assert_eq!(0, popcnt(0b0000));
/// assert_eq!(1, popcnt(0b1000));
/// assert_eq!(1, popcnt(0b0001));
/// assert_eq!(32, popcnt(0xffff_ffff_u64));
/// assert_eq!(64, popcnt(0xffff_ffff_ffff_ffff_u64));
/// ```
#[inline]
pub fn popcnt(val: u64) -> u64 {
    //popcnt_x86_64(val)
    val.count_ones() as u64
}

/// Computes the `popcnt` bit count operation, but skips the first `start_bit` bits in the value
/// before starting the count.
///
/// # Examples
///
/// ```
/// extern crate cqf;
/// use cqf::bitfiddling::popcnt_skip_n;
///
/// assert_eq!(0, popcnt_skip_n(0b0011, 2));
/// assert_eq!(64, popcnt_skip_n(0xffff_ffff_ffff_ffff_u64, 0));
/// assert_eq!(63, popcnt_skip_n(0xffff_ffff_ffff_ffff_u64, 1));
/// assert_eq!(1, popcnt_skip_n(0xffff_ffff_ffff_ffff_u64, 63));
/// assert_eq!(0, popcnt_skip_n(0xffff_ffff_ffff_ffff_u64, 64));
/// ```
#[inline]
pub fn popcnt_skip_n(val: u64, start_bit: u64) -> u64 {
    assert!(start_bit <= 64);
    if start_bit < 64 {
        //Simply mask out the first start_bit bits
        let mask = !bitmask!(start_bit);
        popcnt(val & mask)
    } else {
        //If this 64-bit integer has any bits set after the first 64, it's a most unusual integer
        //indeed
        0
    }
}

/// Counts the number of set bits up to a certain bit position
/// Like the opposite of `popcnt_skip_n`
///
///#Examples
///
/// ```
/// extern crate cqf;
/// use cqf::bitfiddling::popcnt_first_n;
///
/// assert_eq!(2, popcnt_first_n(0b0011, 2));
/// assert_eq!(2, popcnt_first_n(0b0011, 63));
/// assert_eq!(1, popcnt_first_n(0xffff_ffff_ffff_ffff_u64, 0));
/// assert_eq!(2, popcnt_first_n(0xffff_ffff_ffff_ffff_u64, 1));
/// assert_eq!(64, popcnt_first_n(0xffff_ffff_ffff_ffff_u64, 63));
/// assert_eq!(64, popcnt_first_n(0xffff_ffff_ffff_ffff_u64, 64));
/// ```
#[inline]
pub fn popcnt_first_n(val: u64, end_bit: u64) -> u64 {
    assert!(end_bit <= 64);
    //Pretty easy, just mask val so only the first end_bit bits (inclusive)
    //are set
    if end_bit < 63 {
        let mask = (2u64 << end_bit) - 1;
        popcnt(val & mask)
    } else {
        popcnt(val)
    }
}

/// Performs a reverse bit scan, finding the index of the highest set bit.
///
/// #Returns
///
/// `None` if the value has no set bits (that is, if `val` is 0), or the 0-based index of the
/// highest set bit
///
/// # Examples
///
/// ```
/// extern crate cqf;
///
/// use cqf::bitfiddling::*;
///
/// assert_eq!(None, bit_scan_reverse(0));
/// assert_eq!(Some(0), bit_scan_reverse(0b01_u64));
/// assert_eq!(Some(1), bit_scan_reverse(0b11_u64));
/// assert_eq!(Some(63), bit_scan_reverse(0xffff_ffff_ffff_ffff_u64));
/// ```
#[inline]
pub fn bit_scan_reverse(val: u64) -> Option<u64> {
    if val != 0 {
        Some(63 - val.leading_zeros() as u64)
    } else {
        None
    }
}

/// Finds the index of the lowest set bit in the value
///
/// #Returns
///
/// `None` if the value has no set bits (that is, if `val` is 0), or the 0-based index of the
/// lowest set bit
///
/// # Examples
///
/// ```
/// extern crate cqf;
///
/// use cqf::bitfiddling::*;
///
/// assert_eq!(None, bit_scan_forward(0));
/// assert_eq!(Some(0), bit_scan_forward(0x01_u64));
/// assert_eq!(Some(0), bit_scan_forward(0x11_u64));
/// assert_eq!(Some(0), bit_scan_forward(0xffff_ffff_ffff_ffff_u64));
/// assert_eq!(Some(0), bit_scan_forward(0b1111_1111_1111_0000_1111));
/// assert_eq!(Some(1), bit_scan_forward(0b1111_1111_1111_0000_1110));
/// ```
#[inline]
pub fn bit_scan_forward(val: u64) -> Option<u64> {
    if val != 0 {
        Some(val.trailing_zeros() as u64)
    } else {
        None
    }
}

/// Returns the position of the rank'th 1.  (rank = 0 returns the 1st 1)
/// Returns None if there are fewer than rank+1 1s.
///
/// # Examples:
///
/// ```
/// extern crate cqf;
/// use cqf::bitfiddling::*;
///
/// let val = 0b_1000_1111_0001u64;
///
/// assert_eq!(Some(0), bitselect(val, 0)); //0th 1 bit is at position 0
/// assert_eq!(Some(4), bitselect(val, 1)); //1st 1 bit is at position 4
/// assert_eq!(Some(5), bitselect(val, 2)); //2nd 1 bit is at position 5
/// assert_eq!(Some(6), bitselect(val, 3)); //3rd 1 bit is at position 6
/// assert_eq!(Some(7), bitselect(val, 4)); //4th 1 bit is at position 7
/// assert_eq!(Some(11), bitselect(val, 5)); //4th 1 bit is at position 11
/// assert_eq!(None, bitselect(val, 6)); // Note the surprisng return value, '64' means the bit of the specified rank was not found
/// assert_eq!(None, bitselect(val, 7));
/// // ...
/// assert_eq!(None, bitselect(val, 63));
/// ```
#[inline]
pub fn bitselect(val: u64, rank: u64) -> Option<u64> {
    let pos = bitselect_x86_64_bmi2(val, rank);

    if pos != 64 {
        Some(pos)
    } else {
        None
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "bmi2", target_feature = "bmi1"))]
#[inline]
fn bitselect_x86_64_bmi2(val: u64, rank: u64) -> u64 {
    unsafe {
        // This is a novel (to me) use of the pdep instruction
        // The 'mask' parameter to pdep is actually the value we're interested in
        // The 'value' is a mask with a '1' bit in the rank-th position.
        //
        // We run the pdep instruction, then use tzcnt to count the leading zeros which tells us by
        // how many bits the input value was shifted and thus the rank of the rank-th bit.
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
        assert_eq!(0, popcnt(0b000000));
        assert_eq!(1, popcnt(0b000001));
        assert_eq!(1, popcnt(0b100000));
        assert_eq!(2, popcnt(0b100001));
        assert_eq!(32, popcnt(0xffff_ffff_u64));
        assert_eq!(64, popcnt(0xffff_ffff_ffff_ffff_u64));

        let k = 1u64;

        for i in 0..64 {
            assert_eq!(1, popcnt(k << i));
        }

        let k = 0xffff_ffff_ffff_ffff_u64;
        for i in 0..64 {
            assert_eq!(64 - i, popcnt(k.wrapping_shr(i as u32)));
        }
    }

    #[test]
    fn test_popcnt_skip_n() {
        assert_eq!(0, popcnt_skip_n(0b000000, 0));
        assert_eq!(1, popcnt_skip_n(0b000001, 0));
        assert_eq!(0, popcnt_skip_n(0b000001, 1));
        assert_eq!(1, popcnt_skip_n(0b100000, 0));
        assert_eq!(1, popcnt_skip_n(0b100000, 1));
        assert_eq!(1, popcnt_skip_n(0b100000, 5));
        assert_eq!(0, popcnt_skip_n(0b100000, 6));
        assert_eq!(2, popcnt_skip_n(0b100001, 0));
        assert_eq!(1, popcnt_skip_n(0b100001, 1));
        assert_eq!(1, popcnt_skip_n(0b100001, 5));
        assert_eq!(0, popcnt_skip_n(0b100001, 6));
        assert_eq!(1, popcnt_skip_n(0xffff_ffff_ffff_ffff_u64, 63));
        assert_eq!(0, popcnt_skip_n(0xffff_ffff_ffff_ffff_u64, 64));

        let k = 0xffff_ffff_ffff_ffff_u64;

        for i in 0..64 {
            assert_eq!(64 - i, popcnt_skip_n(k, i));
        }
    }

    #[test]
    fn test_popcnt_first_n() {
        assert_eq!(0, popcnt_first_n(0b000000, 0));
        assert_eq!(1, popcnt_first_n(0b000001, 0));
        assert_eq!(1, popcnt_first_n(0b000001, 1));
        assert_eq!(0, popcnt_first_n(0b100000, 0));
        assert_eq!(0, popcnt_first_n(0b100000, 1));
        assert_eq!(1, popcnt_first_n(0b100000, 5));
        assert_eq!(1, popcnt_first_n(0b100000, 6));
        assert_eq!(1, popcnt_first_n(0b100001, 0));
        assert_eq!(1, popcnt_first_n(0b100001, 1));
        assert_eq!(1, popcnt_first_n(0b100001, 2));
        assert_eq!(1, popcnt_first_n(0b100001, 4));
        assert_eq!(2, popcnt_first_n(0b100001, 5));
        assert_eq!(2, popcnt_first_n(0b100001, 6));
        assert_eq!(64, popcnt_first_n(0xffff_ffff_ffff_ffff_u64, 63));
        assert_eq!(64, popcnt_first_n(0xffff_ffff_ffff_ffff_u64, 64));

        let k = 0xffff_ffff_ffff_ffff_u64;

        for i in 0..64 {
            assert_eq!(i + 1, popcnt_first_n(k, i));
        }
    }

    #[test]
    fn test_bit_scan_forward() {
        assert_eq!(None, bit_scan_forward(0));
        assert_eq!(Some(0), bit_scan_forward(0x01));
        assert_eq!(Some(1), bit_scan_forward(0x01_u64 << 1));
        assert_eq!(Some(5), bit_scan_forward(0x01_u64 << 5));
        assert_eq!(Some(9), bit_scan_forward(0x01_u64 << 9));
        assert_eq!(Some(33), bit_scan_forward(0x01_u64 << 33));
        assert_eq!(Some(63), bit_scan_forward(0x01_u64 << 63));
    }

    #[test]
    fn test_bit_scan_reverse() {
        assert_eq!(Some(0), bit_scan_reverse(0b01_u64));
        assert_eq!(Some(1), bit_scan_reverse(0b11_u64));
        assert_eq!(Some(63), bit_scan_reverse(0xffff_ffff_ffff_ffff_u64));
    }

    #[test]
    fn test_bitselect() {
        assert_eq!(None, bitselect(0x0, 0));
        assert_eq!(Some(1), bitselect(0b100010, 0));
        assert_eq!(Some(5), bitselect(0b100010, 1));
        assert_eq!(None, bitselect(0b100010, 2));
    }
}
