//! Implements the logical RSQF operations in a layer one removed from the bit-fiddly details in
//! the Physical layer.
use physical;

#[allow(dead_code)] // for now
#[derive(Default, PartialEq)]
pub struct LogicalData {
    physical: physical::PhysicalData,
}

impl LogicalData {
    pub fn new(nslots: usize, rbits: usize) -> LogicalData {
        LogicalData {
            physical: physical::PhysicalData::new(nslots, rbits),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod logicaldata_tests {}
