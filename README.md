# marzullo

A Rust implementation of the Marzullo algorithm

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
![CI](https://img.shields.io/github/actions/workflow/status/ahmedtadde/marzullo/rust.yml)
![crates.io](https://img.shields.io/crates/d/marzullo)
![docs.rs](https://img.shields.io/docsrs/marzullo)

## Description

Marzullo's algorithm, invented by Keith Marzullo for his Ph.D. dissertation in 1984, is an
agreement algorithm used to select sources for estimating accurate time from a number of noisy
time sources. NTP uses a modified form of this called the Intersection algorithm, which returns
a larger interval for further statistical sampling. However, here we want the smallest interval.
[Here is a more detailed description of the algorithm](https://en.wikipedia.org/wiki/Marzullo%27s_algorithm#Method)

## Credits

This is a port of the [TigerBeetle implementation](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/vsr/marzullo) done mainly by [Joran Dirk Greef](https://github.com/jorangreef) and [King Protty](https://github.com/kprotty).

## License

Licensed under either of

- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

### Contribution

- Contributions are welcome! 🙏
- Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
