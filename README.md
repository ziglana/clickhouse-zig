# clickhouse-zig
clickhouse client for zig 

## Features

- Native ClickHouse protocol implementation
- High performance with zero allocations in hot paths
- Support for complex ClickHouse types
- Connection pooling
- Async query support
- Compression (LZ4, ZSTD)
- Bulk insert optimization
- Streaming support for large result sets
- Comprehensive error handling

## Running Examples

The project includes several examples demonstrating different features. To run a specific example:

```bash
# Build and run an example
zig build run-$EXAMPLE_NAME

# Available examples:
zig build run-basic_connection    # Basic connection and query
zig build run-bulk_insert        # Bulk data insertion
zig build run-streaming          # Streaming large result sets
zig build run-compression        # Data compression
zig build run-transaction        # Transaction handling
zig build run-async_query        # Async query execution
zig build run-materialized_view  # Materialized view creation
zig build run-dictionary         # Dictionary operations
zig build run-distributed_table  # Distributed table setup
zig build run-query_profiling    # Query profiling
zig build run-mutations          # Data mutations
zig build run-sampling          # Data sampling
zig build run-complex_types     # Complex data types
zig build run-pool_config       # Connection pool configuration
zig build run-query_control     # Query monitoring and control
```

Each example demonstrates specific features and includes detailed comments explaining the functionality.