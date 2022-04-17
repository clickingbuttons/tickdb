# tickdb

A way to store billions of ticks (trades and quotes) on disk and query by ticker and/or timestamp

tickdb:
  - minimizes storage requirements for typical equity data
    - uses hashmap for tickers 
    - compresses price data
    - compresses timestamp (when possible)
  - partitions data based on timestamp
    - makes backfilling managable
  - groups data based on ticker
    - makes querying by ticker fast
  - is columnar
    - makes querying columns fast
    - allows adding columns without impacting performance

This database makes 2 key assumptions:
1. Every table has a timestamp column named "ts"
  - This field can be compressed if data is expressed in minutes per day, days per year, etc.
2. Every table has a ticker column named "ticker"
  - You specify up front how many tickers to support

## New table

Table data is saved as binary column files in partition folders.
It's best to use a date format that aligns with how data will be queried and that any single partition can fit in RAM.

Column types:
- Timestamp (required)
  - Int8 = 256 values/frequency
  - Int16 = 65,536 values/frequency
  - Int32 = 4,294,967,296 values/frequency
  - Int64 = 18,446,744,073,709,551,616 values/frequency
- Symbol (required)
  - UInt8 = 255 symbols
  - UInt16 = 65,535 symbols
  - UInt32 = 4,294,967,295 symbols
- Int8
- Int16
- Int32
- Int64
- UInt8
- UInt16
- UInt32
- UInt64
- Float32
- Float64
  - Good for prices when don't need big integers to store large precisions

## New server

- Hardest requirement for DB is storage
- Best recommendation: PCIE4 NVME in RAID
  - If need more slots, use quad m.2 nvme ssd to pci-e 4.0 x16 adapters
  - Total size of all US equity trades from 2004-2021 is 3TB

