// treat warnings as errors
#![deny(warnings)]
mod bitfiddling;

//NOTE: bitarray uses the bitmask! macro from bitfiddling so it must come after bitfiddling in this
//listing
mod bitarray;
mod block;
mod murmur;

mod physical;

mod logical;

pub mod rsqf;
