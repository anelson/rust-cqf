//! Implements the logical RSQF operations in a layer one removed from the bit-fiddly details in
//! the Physical layer.
use physical;

#[allow(dead_code)] // for now
#[derive(Default, PartialEq)]
pub struct LogicalData {
    physical: physical::PhysicalData,
}

#[allow(dead_code)] // for now
type HashValue = physical::HashValue;

/// The type which represents the value of the remainder portion of a `HashValue`.  It is these
/// remainders which are stored in `rbits`-bit slots within the physical data layer.  
/// Note that the actual number of bits stored and retrieved is not 64 but is determiend by the `rbits` runtime parameter.
#[allow(dead_code)] // for now
type RemainderValue = physical::RemainderValue;

/// The type which contains the "quotient" part of a `HashValue`.  The number of bits used to
/// actually represent a quotient is computed in higher layers as the parameter `q` or `qbits`.
#[allow(dead_code)] // for now
type QuotientValue = physical::QuotientValue;

impl LogicalData {
    pub fn new(nslots: usize, rbits: usize) -> LogicalData {
        LogicalData {
            physical: physical::PhysicalData::new(nslots, rbits),
            ..Default::default()
        }
    }

    /// Tests if the filter may contain this quotient and remainder.  Because these values are
    /// derived from a hash function, and the number of bits actually used is much less than 64,
    /// there are possibilities of a collision at higher levels.  However at this layer of
    /// implementation, the existence test is deterministic.  Either this exact `quotient` and
    /// `remainder` exist in the filter, or they do not.  
    ///
    /// At higher levels the test is probabilistic, because the process of deriving these values
    /// from a given input has a potential for collision.  Don't mistake the certainty of the
    /// answer at this level for intended use of this filter structure which remains probabilistic.
    #[inline]
    #[allow(unused_variables)]
    #[allow(dead_code)] // for now
    pub fn contains(&self, quotient: QuotientValue, remainder: RemainderValue) -> bool {
        // If the occupied bit for this quotient isn't set then there's no way this combination of
        // values are present
        match self.physical.find_quotient_run_end(quotient) {
            Some(run_end) => {
                //Quotient is occupied and we have the index of its run end.  We don't actually
                //know where its run starts; it could be at its home slot `quotient` or if that was
                //in use it could be at another higher slot, but never a lower slot.  To find this
                //remainder we simply scan backwards from the runend until we encounter the
                //quotient's home slot, or another slot that is a runend.
                self.physical
                    .scan_for_remainder(run_end, quotient as usize, remainder)
                    .is_some()
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod logicaldata_tests {
    const TEST_SLOTS: usize = 1024 * 64;
    const TEST_RBITS: usize = 9;

    use super::*;

    #[test]
    fn contains_tests() {
        let ld = LogicalData::new(TEST_SLOTS, TEST_RBITS);

        for quotient in 0..TEST_SLOTS {
            assert_eq!(
                false,
                ld.contains(quotient as QuotientValue, 0 as RemainderValue)
            );
        }
    }

    #[test]
    #[should_panic]
    fn contains_panic_on_out_of_range() {
        let ld = LogicalData::new(TEST_SLOTS, TEST_RBITS);

        ld.contains(TEST_SLOTS as QuotientValue, 0);
    }
}
