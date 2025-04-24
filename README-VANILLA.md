# Vanilla Chain Replication

This repository has been converted from CRAQ (Chain Replication with Apportioned Queries) to vanilla Chain Replication.

## Key Changes

1. **Read Model Changes**: In vanilla chain replication, only the tail node serves reads. Non-tail nodes will reject read requests with an error.

2. **Simplified Storage**: The storage layer has been simplified to always return the latest version of an item regardless of its commit status. This is safe because:
   - Only the tail can serve reads in vanilla chain replication
   - The tail always has the latest committed version

3. **Removed CRAQ-specific Logic**: 
   - The `LatestVersion` RPC is deprecated (kept for backward compatibility but logs a warning)
   - The distinction between "dirty" and "clean" items is maintained for the chain commit protocol but no longer affects reads

4. **Enhanced Logging**: Detailed logging has been added to help visualize the flow of writes and commits through the chain

## Write Flow

The write flow remains similar to the original CRAQ implementation:
1. Head node assigns a version number
2. Write propagates down the chain to the tail
3. Tail marks the item committed and initiates commit propagation back up the chain
4. The client receives an acknowledgment only after the write has been committed

## Read Flow

The read flow has been simplified:
1. All read requests must be directed to the tail node
2. Non-tail nodes will reject read requests with an error message
3. The tail node returns the latest version of the requested item

## Benefits of Vanilla Chain Replication

- Simpler implementation and mental model
- No need for coordination between nodes during reads
- Strong consistency guarantees with a clear single source of truth (the tail)

## Limitations Compared to CRAQ

- Lower read throughput (all reads must go to the tail)
- Less read scalability (can't distribute reads across the chain)
- Potential read hotspots on the tail node
