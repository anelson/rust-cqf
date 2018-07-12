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

        let index = quotient as usize;

        // The task is to find the first unused slot, starting at `quotient` and if that's in use
        // then scanning forward until the first unused slot.
        //
        // Note that if there is already a run for `quotient`, we add this remainder to the run,
        // but that may require shifting subsequent runs if they are immediately adjacent to this
        // one.
        if self.physical.is_slot_empty(index) {
            //This is the easy case.  The quotient's home slot is not in use at all, so put the
            //remainder directly in that slot.
            self.physical.set_slot(index, remainder);
            self.physical.set_occupied(index, true);
            self.physical.set_runend(index, true);
            return ();
        }

        //The easy case has been eliminated so now pursue the general case.
        //Find a slot at `quotient` or possibly the first unused slot after it to store this
        //remainder.
        let runend_index = self.physical.find_run_end(index);

        //We will insert the new value at the end of the run.  Of course, it's possible that this
        //slot is currently in use by a subsequent run.  That doesn't matter; we will shift those
        //subsequent values if needed to make room here, because we have an invariant to uphold
        let insert_index = runend_index + 1;

        //Figure out where the next empty slot is so we can shift everything from insert_into
        //onwards into that empty slot
        let empty_slot_index = self.physical.find_first_empty_slot(insert_index);

        //TODO: handle this full scenario more gracefully
        let empty_slot_index =
            empty_slot_index.expect("the underlying data structure has no free slots left!");

        //Shift all of the remainder values and runends between insert_index and empty_slot_index
        //to the left by one.  That will overwrite the contents at empty_slot_index (which is emtpy
        //so that's fine), and make room at insert_index for this remainder and runend bit
        self.physical
            .shift_slots_left(insert_index, empty_slot_index, remainder);
        self.physical
            .shift_runends_left(insert_index, empty_slot_index - 1, true);

        //The new runend is at `insert_index`, which is one slot after the old runend at
        //`runend_index`.  Clear that old runend bit
        self.physical.set_runend(runend_index, false)

        //TODO: Update the block offsets
    }

    /// Performs a comprehensive (and slow!) scan of the internal structure verifying all
    /// invariants and checking the integrity of the structure.  
    ///
    /// # Panics
    ///
    /// Panics if there is any validation error with the content of the structure
    #[cfg(test)] //This is used in the tests only
    pub fn validate_contents(&self) -> () {
        assert!(self.qbits > 1);
        assert!(self.rbits >= 3);

        //There should be the saself.number of runends as occupieds in total
        let total_runends = self.physical.count_runends(0, self.physical.len());
        let total_occupieds = self.physical.count_occupieds(0, self.physical.len());

        assert_eq!(
            total_runends, total_occupieds,
            "the runend and occupied bit counts don't match"
        );

        //The first occupied bit should be at or before the position of the first runend
        for i in 0..self.physical.len() {
            if self.physical.is_occupied(i) {
                break;
            } else {
                assert!(
                    !self.physical.is_runend(i),
                    "the first set runend bit is in slot {} before the first occupied bit!",
                    i
                );
            }
        }

        //The last runend bit should be at or after the position of the last occupied
        for i in (0..self.physical.len()).rev() {
            if self.physical.is_runend(i) {
                break;
            } else {
                assert!(
                    !self.physical.is_occupied(i),
                    "the last set occupied bit is in slot {} before the last runend bit!",
                    i
                );
            }
        }
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

    /// Test inserts for multiple remainders in one quotient requiring shifting of values
    #[test]
    pub fn insert_with_shifting_test() {
        // Test cases to exercise this scenarios
        // Each test case is a vector of quotients and their remainders.  The quotients and their
        // remainders are inserted in order, and after every operation the invariants of the
        // structure are verified.
        let test_cases: Vec<Vec<(u64, Vec<u64>)>> = vec![
            // Just insert a single quotient and remainder; simply and inoffensive
            vec![(100, vec![1])],
            // Insert two quotients each with a remainder, right next to each other
            vec![(100, vec![1]), (101, vec![2])],
            // Insert one quotient with two remainders
            vec![(100, vec![1, 2])],
            // Insert a quotient with two remainders then another quotient in the next slot
            vec![(100, vec![1, 2]), (101, vec![3])],
        ];

        for (index, ref test) in test_cases.iter().enumerate() {
            let mut ld = LogicalData::new(TEST_QBITS, TEST_RBITS);

            for &(ref quotient, ref remainders) in *test {
                let context = format!(
                    "test index={} quotient={} remainders={:?}",
                    index, *quotient, *remainders
                );

                ld.validate_contents();

                assert_eq!(
                    false,
                    ld.physical.is_occupied(*quotient as usize),
                    "context=[{}]",
                    context
                );

                for r in remainders {
                    assert_eq!(
                        false,
                        ld.contains(*quotient, *r),
                        "context=[r={} {}]",
                        *r,
                        context
                    );
                    ld.insert(*quotient, *r);
                    ld.validate_contents();
                    assert_eq!(
                        true,
                        ld.contains(*quotient, *r),
                        "context=[r={} {}]",
                        *r,
                        context
                    );
                }
            }

            //Now all the inserts are done; make sure that hasn't broken anything
            for &(ref quotient, ref remainders) in *test {
                let context = format!(
                    "test index={} quotient={} remainders={:?}",
                    index, *quotient, *remainders
                );

                assert_eq!(
                    true,
                    ld.physical.is_occupied(*quotient as usize),
                    "context=[{}]",
                    context
                );

                for r in remainders {
                    assert_eq!(true, ld.contains(*quotient, *r), "context=[{}]", context);
                }
            }
        }
    }
}
