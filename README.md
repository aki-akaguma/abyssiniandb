# abyssiniandb

[![crate][crate-image]][crate-link]
[![Docs][docs-image]][docs-link]
![Rust Version][rustc-image]
![Apache2/MIT licensed][license-image]
[![Test][test-image]][test-link]

The simple local key-value store.

## Features

- key-value store.
- hash buckets algorithm.
- minimum support rustc 1.58.1 (db9d1b20b 2022-01-20)

## Compatibility

- Nothing?

## Todo

- [ ] more performance
- [ ] DB lock as support for multi-process-safe

## Low priority todo

- [ ] transaction support that handles multiple key-space at a time.
- [ ] thread-safe support
- [ ] non db lock multi-process-safe support

## Examples

# Changelogs

[This crate's changelog here.](https://github.com/aki-akaguma/abyssiniandb/blob/main/CHANGELOG.md)

# License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   https://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   https://opensource.org/licenses/MIT)

at your option.

[//]: # (badges)

[crate-image]: https://img.shields.io/crates/v/abyssiniandb.svg
[crate-link]: https://crates.io/crates/abyssiniandb
[docs-image]: https://docs.rs/abyssiniandb/badge.svg
[docs-link]: https://docs.rs/abyssiniandb/
[rustc-image]: https://img.shields.io/badge/rustc-1.58+-blue.svg
[license-image]: https://img.shields.io/badge/license-Apache2.0/MIT-blue.svg
[test-image]: https://github.com/aki-akaguma/abyssiniandb/actions/workflows/test.yml/badge.svg
[test-link]: https://github.com/aki-akaguma/abyssiniandb/actions/workflows/test.yml
