//! Implements the logical RSQF operations in a layer one removed from the bit-fiddly details in
//! the Physical layer.
use physical;

#[allow(dead_code)] // for now
#[derive(Default, PartialEq)]
pub struct LogicalData {
    physical: physical::PhysicalData,
    qbits: usize,
    rbits: usize,
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
    /// Initializes a new `LogicalData` structure as well as an underlying `PhysicalData` structure
    /// to contain the filter contents.
    ///
    /// # Arguments
    ///
    /// `qbits` - The number of bits to use in the `quotient` portion of each hash value.  The
    /// structure will allocate `2^q` slots, where each slot will be `rbits` bits, so selection of
    /// this parameter is based on a desired `rbits` value combined with a number of expected
    /// values.
    /// `rbits` - The number of bits to use in the `remainder` portion of each hash value.  It's
    /// expected that `q` plus `rbits` together will be less than 64.
    pub fn new(qbits: usize, rbits: usize) -> LogicalData {
        assert!(qbits + rbits <= 64);

        let nslots = 1usize << qbits;

        LogicalData {
            physical: physical::PhysicalData::new(nslots, rbits),
            qbits,
            rbits,
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
        self.validate_quotient(quotient);
        self.validate_remainder(remainder);

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

    #[allow(unused_variables)]
    #[allow(dead_code)] // for now
    pub fn insert(&mut self, quotient: QuotientValue, remainder: RemainderValue) -> () {
        self.validate_quotient(quotient);
        self.validate_remainder(remainder);

        // The task is to find the first unused slot, starting at `quotient` and if that's in use
        // then scanning forward until the first unused slot.
        //
        // Note that if there is already a run for `quotient`, we add this remainder to the run,
        // but that may require shifting subsequent runs if they are immediately adjacent to this
        // one.
        if self.physical.is_slot_empty(quotient as usize) {
            //This is the easy case.  The quotient's home slot is not in use at all, so put the
            //remainder directly in that slot.
            self.physical.set_slot(quotient as usize, remainder);
            self.physical.set_occupied(quotient as usize, true);
            self.physical.set_runend(quotient as usize, true);
            return ();
        }

        panic!("NYI!");
    }

    /// In debug builds, ensures quotient values are masked out properly and only contain the
    /// expected number of bits.
    #[inline]
    fn validate_quotient(&self, q: QuotientValue) -> () {
        debug_assert!(
            (q & !bitmask!(self.qbits)) == 0,
            "quotient {:x} has more than {} significant bits",
            q,
            self.qbits
        );
    }

    /// In debug builds, ensures remainder values are masked out properly and only contain the
    /// expected number of bits.
    #[inline]
    fn validate_remainder(&self, r: RemainderValue) -> () {
        debug_assert!(
            (r & !bitmask!(self.rbits)) == 0,
            "remainder {:x} has more than {} significant bits",
            r,
            self.rbits
        );
    }
}

#[cfg(test)]
mod logicaldata_tests {
    const TEST_QBITS: usize = 16;
    const TEST_SLOTS: usize = 1usize << TEST_QBITS;
    const TEST_RBITS: usize = 9;

    use super::*;

    #[test]
    fn contains_tests() {
        let ld = LogicalData::new(TEST_QBITS, TEST_RBITS);

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
        let ld = LogicalData::new(TEST_QBITS, TEST_RBITS);

        ld.contains(TEST_SLOTS as QuotientValue, 0);
    }

    /// Tests the very simple insert pathway in which the home slot of the quotient to be inserted
    /// is unused.
    #[test]
    pub fn insert_simple_case_test() {
        let mut ld = LogicalData::new(TEST_QBITS, TEST_RBITS);

        for quotient in 0..TEST_SLOTS as QuotientValue {
            let remainder = quotient & bitmask!(TEST_RBITS);

            assert_eq!(false, ld.contains(quotient, remainder));
            ld.insert(quotient, remainder);
            assert_eq!(true, ld.contains(quotient, remainder));

            //But it does NOT contain any other remainder for this quotient
            assert_eq!(false, ld.contains(quotient, remainder ^ 0xff));
        }
    }
}
