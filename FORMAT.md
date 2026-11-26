# ParityBay v1 on disk

A cluster root contains a single current catalog and the fragment/staging
files named by it. Paths in receipts are relative to the root. The catalog
is always written in v1 and is read by the same loader at initialization,
open, query and transaction time. `v1-check` and `v1-export` inspect or
copy this format.

## Frame

| Field | Encoding |
|---|---|
| Magic | 8 bytes, ASCII `PBAYV1` followed by CR LF |
| Payload length | `u32` |
| Payload CRC32C | `u32`, Castagnoli checksum of the payload |
| Payload | Exactly the declared number of bytes |

All multi-byte numeric fields use little endian. `str` means `u16` byte
length followed by UTF-8 bytes. `array<T>` means a `u32` count followed by
that many encoded entries. Fixed arrays have no length prefix. Tables are
ordered by primary key in the canonical writer. The reader rejects
duplicate keys, unknown tags, invalid layouts, out-of-bounds counts,
truncation and trailing bytes. A complete frame is limited to 64 MiB.

The trailing completion array records the receipt of a published multipart
upload by upload ID. The canonical writer omits it entirely when the table is
empty, so catalogs produced before completions existed (and catalogs with no
published multipart uploads) keep their byte layout. A reader that finds
trailing bytes must accept them as this array; an absent array is an empty
table.

## Payload

```text
catalog = block_bytes:u32 staging_limit:u64 next_id:u64
          array<node> array<upload> array<object> array<repair> array<retired>
          [array<completion>]

node = id:str domain:str media:u8 capacity:u64 state:u8

upload = id:u64 key:str expected_bytes:u64 media:u8 domains:u8 condition
         array<part> array<placement>
condition = tag:u8 [generation:u64 for tag 2]
part = number:u32 revision:u64 bytes:u32 crc32c:u32
placement = node0:str node1:str node2:str

object = key:str generation:u64 size:u64 media:u8 domains:u8 array<stripe>
stripe = index:u32 offset:u64 data0_bytes:u32 data1_bytes:u32
         fragment0 fragment1 fragment2
fragment = shard:u8 node:str revision:u64 bytes:u32 crc32c:u32

repair = id:u64 key:str generation:u64 stripe:u32 from:fragment to:fragment
         kind:u8 state:u8 detail:str
retired = node:str generation:u64 stripe:u32 shard:u8 revision:u64 bytes:u32
completion = upload:u64 key:str generation:u64 bytes:u64 stripes:u32
             condition [replaced:u8 [generation:u64 for flag 1]]
             weaker_stripes:u32
```

| Field | Tags |
|---|---|
| Media | 0 hot, 1 capacity |
| Domains | 0 strict, 1 prefer |
| Node state | 0 online, 1 draining, 2 offline, 3 removed |
| Write condition | 0 any, 1 absent, 2 exact generation |
| Repair kind | 0 heal, 1 rebalance |
| Repair state | 0 planned, 1 published, 2 stale, 3 unrecoverable |

A fragment consists of raw bytes in
`nodes/NODE/fragments/GGGGGGGGGGGGGGGG-SSSSSSSS-H-RRRRRRRRRRRRRRRR.pb`.
`G`, `S` and `R` are zero-padded lowercase hexadecimal generation,
stripe and revision. `H` is a shard number 0–2. Staged parts live at
`uploads/UUUUUUUUUUUUUUUU/PPPPPPPP-RRRRRRRRRRRRRRRR.part` with the same
number encoding. The file length and CRC are stored in the receipt.

The logical stripe width is twice `block_bytes`. In a short stripe shard
1 can be shorter than shard 0, including zero bytes. XOR treats absent tail
bytes of shard 1 as zeros, stores parity to the length of shard 0 and
truncates a recovered shard to the length in its receipt. Each stripe
physically stores three files. A zero-length object has no stripes and
reserves no fragment bytes.

The catalog sequence allocates upload IDs, direct PUT generations, staged
part revisions and repair IDs. A multipart object uses its upload ID as
its generation. Initial fragments use the object generation as their
revision. A repair uses its own ID as the target revision and compares its
expected original receipt with the current mapping before publication.
