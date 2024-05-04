# 🧰 Conduit Toolbox 🦀

Helper tools for [Conduit](https://conduit.rs), a matrix homeserver in rust.

This repository provides binaries to wrangle your conduit homeserver with.

## Tools

### `conduit_sled_to_sqlite`

**This tool is no longer available on the main branch**, go to `with-sled` to download from there.

### `conduit_migrate`

This tool provides generic migration between `heed`, `sqlite`, `persy`, and `rocksdb` conduit databases.

## Installing

For the best experience, compile this toolbox locally on your server;

1. Download and install rust, see [rustup](https://rustup.rs/) for more.
2. Be sure that the rust executables are on your `$PATH`
3. You may want to have a compiler and build tools installed on your system, or else cargo will complain about not being able to "link" or "compile" with `cc`.
   - on debian/ubuntu-based systems you can install this with `sudo apt install build-essential`
4. `cargo install --locked --git https://github.com/shadowjonathan/conduit_toolbox conduit_migrate`

(updating only requires running that last line again)
