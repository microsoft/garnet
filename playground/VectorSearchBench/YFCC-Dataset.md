# YFCC10M Filtered Vector Search Dataset

A curated subset of the [YFCC100M](https://multimediacommons.wordpress.com/yfcc100m-core-dataset/) dataset, prepared for benchmarking **filtered approximate nearest neighbor (ANN)** search. The dataset provides uint8 image descriptor vectors with structured metadata attributes, enabling evaluation of vector search engines under varying filter selectivity.

## Origin

The YFCC100M (Yahoo Flickr Creative Commons 100 Million) dataset contains metadata for ~100 million Flickr photos and videos. This derivative provides:

- **Image descriptors** extracted as 192-dimensional uint8 vectors
- **Structured attributes** (year, month, camera manufacturer, country) derived from EXIF and geo metadata
- **Pre-built query sets** with filters at controlled selectivity levels
- **Pre-computed ground truth** for each query set

## File Inventory

### Base Vectors

| File | Records | Size | Description |
|------|---------|------|-------------|
| `base.1M.u8bin` | 1,000,000 | 184 MB | 1M base vectors (uint8, 192 dims) |
| `base.1M.label.jsonl` | 1,000,000 | 69 MB | Attributes for the 1M subset |
| `base.10M.u8bin` | 10,000,000 | 1.8 GB | 10M base vectors (uint8, 192 dims) |
| `base.10M.label.jsonl` | 10,000,000 | 759 MB | Attributes for the 10M subset |

### Query Vectors (10,000 queries each)

| File | Size | Filter Type |
|------|------|-------------|
| `single_high_query_10k.u8bin` | 1.9 MB | Single filter, high selectivity (~15%) |
| `single_medium_query_10k.u8bin` | 1.9 MB | Single filter, medium selectivity (~1.3%) |
| `single_low_query_10k.u8bin` | 1.9 MB | Single filter, low selectivity (~0.02%) |
| `multiple_medium_query_10k.u8bin` | 1.9 MB | Multi-filter, medium selectivity (~1.5%) |
| `query.10k.u8bin` | 1.9 MB | Multi-filter, low selectivity (~0.02%) |
| `query.public.100K.u8bin` | 19 MB | 100K unfiltered queries |

### Filter Labels (JSONL)

| File | Size | Description |
|------|------|-------------|
| `single_high_query_10k.label.jsonl` | 557 KB | Year equality filters |
| `single_medium_query_10k.label.jsonl` | 565 KB | Country equality filters |
| `single_low_query_10k.label.jsonl` | 565 KB | Rare camera/country filters |
| `multiple_medium_query_10k.label.jsonl` | 952 KB | Camera AND year filters |
| `multiple_low_query_10k.label.jsonl` | 946 KB | Country AND year filters |

### Ground Truth

| File | Size | Description |
|------|------|-------------|
| `GT_single_high_query_10k.bin` | 781 KB | GT for single-high (10K queries × 10 neighbors) |
| `GT_single_medium_query_10k.bin` | 781 KB | GT for single-medium |
| `GT_single_low_query_10k.bin` | 781 KB | GT for single-low |
| `GT_multiple_medium_query_10k.bin` | 781 KB | GT for multi-medium |
| `GT_multiple_low_query_10k.bin` | 781 KB | GT for multi-low |
| `GT_100k_1M.bin` | 7.6 MB | Unfiltered GT (100K queries vs 1M base) |
| `GT_100k_10M.bin` | 7.6 MB | Unfiltered GT (100K queries vs 10M base) |
| `GT.public.1M.bin` | 76 MB | Extended unfiltered GT |

### Configuration

| File | Description |
|------|-------------|
| `yfcc-config.json` | Maps phase names to data files (used by VectorSearchBench) |

## Binary Formats

### Vector files (`.u8bin`)

```
[int32 num_vectors][int32 dimensions][uint8 × dimensions × num_vectors]
```

- First 8 bytes: two little-endian int32 values (count, dims)
- Remaining bytes: row-major uint8 vector data
- Example: 1M vectors × 192 dims = 8 + (1,000,000 × 192) = 192,000,008 bytes

### Label files (`.label.jsonl`)

One JSON object per line, positionally aligned with the vector file.

**Base labels** — one per base vector:
```json
{"doc_id": 0, "year": "2010", "month": "May", "camera": "Panasonic", "country": "US"}
{"doc_id": 1, "year": "2011", "month": "July", "camera": "Canon"}
{"doc_id": 2, "year": "2011", "month": "July", "camera": "Canon"}
```

**Query filter labels** — one per query vector:
```json
{"query_id": 0, "filter": {"year": {"$eq": "2009"}}}
{"query_id": 1, "filter": {"$and": [{"camera": {"$eq": "NIKON"}}, {"year": {"$eq": "2006"}}]}}
```

Note: Not all base records have all attributes. The `country` field is absent for photos without geolocation data.

### Ground truth files (`.bin`)

```
[int32 num_queries][int32 num_neighbors]
[int32 × num_neighbors × num_queries]    // neighbor IDs (0-based doc_id)
```

- First 8 bytes: two little-endian int32 values
- Remaining: row-major int32 neighbor IDs, `num_neighbors` per query
- Example: 10K queries × 10 neighbors = 8 + (10,000 × 10 × 4) = 400,008 bytes... actual files are 800,008 bytes (10 neighbors stored as int64 or with distances)

## Attribute Schema

| Attribute | Type | Example Values | Coverage |
|-----------|------|---------------|----------|
| `year` | string | `"2005"` – `"2014"` | 100% |
| `month` | string | `"January"` – `"December"` | 100% |
| `camera` | string | `"Canon"`, `"Nikon"`, `"Panasonic"`, ... | ~95% |
| `country` | string | ISO 3166-1 alpha-2 (`"US"`, `"JP"`, `"FR"`, ...) | ~40% |

All attribute values are strings (even year), matching the RESP protocol's string-only attribute model.

## Filter Selectivity Profiles

Measured against the first 100K base records:

| Phase | Filter Pattern | Example | Avg Selectivity | Avg Matching Records |
|-------|---------------|---------|-----------------|---------------------|
| single-high | `.year == "YYYY"` | `.year == "2009"` | 15.45% | 15,448 / 100K |
| single-medium | `.country == "XX"` | `.country == "ES"` | 1.28% | 1,281 / 100K |
| single-low | `.camera == "X"` or `.country == "XX"` | `.country == "JM"` | 0.02% | 16 / 100K |
| multi-medium | `.camera == "X" && .year == "YYYY"` | `.camera == "NIKON" && .year == "2006"` | 1.48% | 1,483 / 100K |
| multi-low | `.country == "XX" && .year == "YYYY"` | `.country == "CA" && .year == "2005"` | 0.02% | 24 / 100K |

**Selectivity** = fraction of base vectors whose attributes satisfy the filter. Computed per-query by counting matching records in the base set, then averaged across all 200 benchmark queries per phase.

## Filter Syntax

Filters in the JSONL files use MongoDB-style operators:

| JSONL Filter | Garnet/Redis Dot-Syntax |
|-------------|------------------------|
| `{"year": {"$eq": "2009"}}` | `.year == "2009"` |
| `{"$and": [{"camera": {"$eq": "NIKON"}}, {"year": {"$eq": "2006"}}]}` | `.camera == "NIKON" && .year == "2006"` |

The VectorSearchBench tool converts MongoDB-style filters to dot-syntax before issuing VSIM commands.

## Config File (`yfcc-config.json`)

```json
{
  "base": {
    "vectors": "base.1M.u8bin",
    "labels": "base.1M.label.jsonl"
  },
  "unfilteredQueryVectors": "single_high_query_10k.u8bin",
  "phases": [
    { "name": "single-high",   "vectors": "single_high_query_10k.u8bin",     "filters": "single_high_query_10k.label.jsonl" },
    { "name": "single-medium", "vectors": "single_medium_query_10k.u8bin",   "filters": "single_medium_query_10k.label.jsonl" },
    { "name": "single-low",    "vectors": "single_low_query_10k.u8bin",      "filters": "single_low_query_10k.label.jsonl" },
    { "name": "multi-medium",  "vectors": "multiple_medium_query_10k.u8bin", "filters": "multiple_medium_query_10k.label.jsonl" },
    { "name": "multi-low",     "vectors": "query.10k.u8bin",                 "filters": "multiple_low_query_10k.label.jsonl" }
  ]
}
```

All file paths are relative to `--data-dir`. Change `base.vectors` and `base.labels` to `base.10M.*` to benchmark against the full 10M dataset.

## Download

> **TODO**: Upload to Azure Blob Storage and add download URLs here.

The dataset files total approximately 3 GB (1M subset) or 6 GB (including 10M). Contact the dataset maintainer for access.

## Quick Start

```bash
# 1. Place dataset files in a directory (e.g., C:\data\yfcc)
# 2. Ensure yfcc-config.json is in the VectorSearchBench output directory
#    (or specify --yfcc-config path)

# 3. Start Garnet with vector support
dotnet run --project main/GarnetServer -- --port 6380 --enable-vector-set-preview

# 4. Start Redis 8
docker run -d --name redis8 -p 6379:6379 redis:8.0.0

# 5. Run benchmark (100K vectors from 1M base)
dotnet run --project playground/VectorSearchBench -- \
  --data-dir "C:\data\yfcc" \
  --yfcc-count 100000 \
  --num-queries 200 --top-k 10 --ef 200

# 6. Run with full 1M base (edit yfcc-config.json base to use base.1M.*)
dotnet run --project playground/VectorSearchBench -- \
  --data-dir "C:\data\yfcc" \
  --num-queries 200 --top-k 10 --ef 200
```

## Generating the Dataset

The JSONL label files were converted from the original `.label` text format using:

```bash
cargo run --package label-filter --example converter \
  "base.10M.label.txt" \
  "<query_file>.label" \
  "base.10M.jsonl" \
  "<query_file>.label.jsonl"
```

The `label-filter` crate is part of the DiskANN Rust toolchain.

## References

- [YFCC100M Core Dataset](https://multimediacommons.wordpress.com/yfcc100m-core-dataset/)
- [Filtered-DiskANN (NeurIPS 2023)](https://arxiv.org/abs/2307.11013) — the filtered ANN algorithm
- Garnet VectorSearchBench: `playground/VectorSearchBench/`
