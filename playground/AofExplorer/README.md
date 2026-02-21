# AOF Browser

A C# tool for browsing and analyzing Garnet AOF (Append-Only File) log files.

## Overview

This browser can read and decode Garnet AOF files, which use the TsavoriteLog format. It supports:

- Basic and extended AOF headers
- Store operations (Upsert, RMW, Delete)
- Object store operations  
- Transaction markers
- Stored procedures
- Checksum verification

## File Format

Each AOF file starts with a 64-byte TsavoriteLog header, followed by log entries:

```
[64-byte file header]
[Entry 1: TsavoriteLog header + AOF header + data]
[Entry 2: TsavoriteLog header + AOF header + data]
...
```

### Entry Structure

```
[TsavoriteLog Header] [AofHeader/AofExtendedHeader] [Key] [Value] [Input]
```

- **TsavoriteLog Header**: 4-12 bytes (length + optional checksum)
- **AofHeader**: 16 bytes (basic) or **AofExtendedHeader**: 25 bytes (sharded)
- **Key/Value**: Variable length SpanByte structures
- **Input**: Variable length operation-specific data

## Usage

```bash
dotnet run <aof_file_path> [options]
```

### Options

- `--checksum-type <none|per-entry>`: Checksum verification type (default: none)
- `--extended-headers`: Use extended headers for sharded logs
- `--max-entries <number>`: Limit number of entries to display
- `--verbose`: Show detailed entry information

### Examples

```bash
# Browse first 10 entries with detailed output
dotnet run ../aof.0.log.0 --verbose --max-entries 10

# Analyze with checksum verification
dotnet run ../aof.0.log.0 --checksum-type per-entry

# View extended header format for sharded logs
dotnet run ../aof.0.log.0 --extended-headers --verbose
```