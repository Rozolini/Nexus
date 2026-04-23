# On-disk storage format

This document is the low-level format reference for the Nexus storage
layer: the on-disk `Record` layout, segment file structure, the data
directory manifest, and the block partitioning used for corruption
detection. It is intentionally limited to the storage substrate; the
planner, scheduler, and benchmark harness are documented elsewhere.

## Record (v1, on disk)

Little-endian.

| Field        | Size     | Notes                                      |
|-------------|----------|--------------------------------------------|
| `key`       | 16 bytes | `u128`                                     |
| `version`   | 8 bytes  | `u64`                                      |
| `flags`     | 4 bytes  | `u32`; bit 0 = tombstone                   |
| `payload_len` | 4 bytes | `u32`                                     |
| `payload`   | `payload_len` | arbitrary bytes                        |
| `checksum`  | 4 bytes  | CRC32 over header ‖ payload (IEEE)       |

Total size: `32 + payload_len + 4`.

### In-memory (`storage::record::Record`)

The in-memory representation carries the same fields as the on-disk
encoding: `key`, `version`, `flags`, `payload_len`, `payload`,
`checksum`. The invariant `payload_len == payload.len()` is enforced in
`new` / `decode` and is re-checked via `debug_assert` on encode and
header construction.

## Segment file

1. **Header** (64 bytes): magic `NEXSEG01`, format `u32`, segment id
   `u64`, zero padding.
2. **Records** back-to-back starting at offset 64.
3. **Footer**: magic `NEXFOOTB`, `u64` count, `count × u64` absolute
   record start offsets, and a CRC32 over the footer prefix (excluding
   the final CRC field itself).

Recovery proceeds as follows: if the footer is present and its CRC
validates, it is used directly. Otherwise the file is scanned from
offset 64, records are decoded until a decode or CRC failure, the file
is truncated to the last successfully decoded byte, and a new footer is
written. The procedure is deterministic for a given byte sequence.

## Manifest

The data directory contains `MANIFEST.json` with the shape
`{ "version": 1, "segments": ["000001.seg", "000002.seg"] }`.

Updates are written via `MANIFEST.json.tmp` and then renamed into place.
On Windows the destination is deleted before the rename so that the
operation behaves consistently across platforms.

## Blocks

File offsets are partitioned into logical blocks (default 64 KiB).
Records may span block boundaries. Corruption within a block is
detected when validating the CRC of any record whose bytes cover the
affected region.
