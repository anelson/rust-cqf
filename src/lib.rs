// treat warnings as errors
#![deny(warnings)]
pub mod bitfiddling;

//NOTE: bitarray uses the bitmask! macro from bitfiddling so it must come after bitfiddling in this
//listing
pub mod bitarray;
pub mod murmur;

pub mod block;
pub mod physical;
pub mod rsqf;
