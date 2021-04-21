# Change Log

# v0.4.0 - 21 Apr, 2021
- The query result limit is now configurable. It defaults to 100,000 rows to prevent the loading
  of a too large number of results into memory, but can be increased or decreased per connection.
- Bug fix: The UI field for Emulator port accepted a string. It is now required to be a number.

# v0.3.0 - 16 Apr, 2021
- Adds support for DDL statements
- Adds support for connecting to Spanner Emulator

# v0.2.0 - 3 March, 2021
- Initial release
