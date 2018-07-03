// treat warnings as errors
#![deny(warnings)]
mod bitfiddling;

//NOTE: bitarray uses the bitmask! macro from bitfiddling so it must come after bitfiddling in this
//listing
mod bitarray;
mod murmur;

mod block;
mod physical;
pub mod rsqf;
