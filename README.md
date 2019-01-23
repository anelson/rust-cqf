# (R)CQRF, counting quotient filter

I based this on a Go implementation by `nfisher` (linked below).  It's not complete yet and doesn't even pass its test
harness, so don't use it.

# Notes from the CQF code

## Key terms

* `n` - The number of expected insertions.  Note this isn't unique values; it's all insertion operations
* `δ` - The false positive rate as a fraction like `1/64` or `1/512`
* `p` - The number of bits from the hash function which is used to actually look up values.  This is computed based on
  the desired false positive rate `δ` and the insertion count `n` thusly:

  `p = log2(n/δ)`
* `q` - The number of bits of `p` which are used as the quotient to determine the home slot where a given hash value
  should go
* `r` - The bits of `p` left over after the `q` bits have been used to find the home slot.  This remainder is used to
  distinguish between multiple values that have the same quotient.  Because of the probabilistic nature of this data
  structure, the worst case is that two hashes `h(x)` and `h(y)` are disambiguated entirely based on the low `r` bits of
  their hashes, and thus the worst case false positive rate is `2^-r`.

  So for `r = 6`, the false positive rate is `1/(2^6)` which is `1/64`
* `BITS_PER_SLOT` - In the code sometimes this is used for `r`.  The code uses conditional compilation to choose either
  compile-time or run-time implementation.  For values `8`, `16`, `32`, and `64` there are accelerated implementations
  that use the corresponding integer types.

# References

* [Go implementation that's a bit more readable](https://github.com/nfisher/rsqf)
* [Morning Paper explainer for the actual
  paper](https://blog.acolyer.org/2017/08/08/a-general-purpose-counting-filter-making-every-bit-count/)
* [The actual paper](https://www3.cs.stonybrook.edu/~ppandey/files/p775-pandey.pdf)

# License

`rust-cqf` is dual-licensed under the Apache 2.0 and MIT licenses.

