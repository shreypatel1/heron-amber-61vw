# Shopping Sessions

Native C++ pipeline for the Databricks Shopping Sessions track: mmap + parallel SIMD CSV parsing, radix partitioning for dedup and per-person sessionization, hand-written Parquet writer. Static binary in a `FROM scratch` image.
