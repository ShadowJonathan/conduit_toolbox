# 🧰 Conduit Toolbox 🦀

Helper tools for [Conduit](https://conduit.rs), a matrix homeserver in rust.

This repository provides binaries to wrangle your conduit homeserver with.

## Tools

### `conduit_sled_to_sqlite`

A one-shot migration tool to convert your sled database to a sqlite one;

Instructions: (read the notes below first)
1. execute `conduit_sled_to_sqlite <database_directory>`
    - `conduit_sled_to_sqlite /var/lib/matrix-conduit/conduit_db`
2. the console will be spammed with a bunch of numbers, this is for diagnostic purposes, when the command returns, your database has been converted.

Some notes:
- The conversion process can take a second depending on the speed of the disk of your server, on raspberry pis it could take a while.
- **Have your server turned off** throughout the conversion process, be sure it is updated to a version which uses SQLite as the main database, and only then turn it back on
  - If you have the server be on during the conversion process, some data might be missing from the new database
  - If you turn the server back on with an older version (which still uses sled), it'll create a brand new sled database in the database directory, which might be confusing.
- The above method places the new database in the same directory as the old one, this'll effectively duplicate your database, if you're sure your server is up-to-date and works with the new database, you can remove the old sled database files under that directory (be sure you have a backup first!);
  - `blobs/`
  - `db`
  - `conf`
- The new database can be anywhere from 2 to 4 times bigger than the old database, if you want to know how much your current database size is, run the following command;
  - `du --apparent-size --max-depth=0 -h <database_directory>`
  - And if you want to know how much room you have on your disk, you can run this;
    - `df -h <database_directory>/` (with that trailing slash)
  - Make sure you have this disk-space *extra* on your machine, else the conversion process will fill it up.

## Installing

For the best experience, compile this toolbox locally on your server;

1. Download and install rust, see [rustup](https://rustup.rs/) for more.
2. Be sure that the rust executables are on your `$PATH`
3. `cargo install --locked --git https://github.com/shadowjonathan/conduit_toolbox`

(updating only requires running that last line again)
