# Consistent Hashing in C#

A simple exploration of consistent hashing implemented in C#.

## Purpose

This repository was created to explore and understand the
fundamentals of consistent hashing algorithms.

## What is Consistent Hashing?

Consistent hashing is a technique used in distributed systems
to distribute data across multiple nodes in a way that minimizes
remapping when nodes are added or removed.
Unlike traditional hashing, where changing the number of nodes
requires remapping most keys, consistent hashing only affects
a small portion of keys.

## Implementation

The `HashRing` class provides a basic consistent hashing implementation with the following features:

- **Thread-safe operations** using `ReaderWriterLockSlim`
- **32-bit hash space** using the first 4 bytes of SHA256 hash output
- **SHA256 hashing** for key distribution
- **Binary search** for efficient node lookup
- **virtual nodes** for better key distribution
- **Wrap-around behavior** for proper ring semantics

## Usage Example

```csharp
var hashRing = new HashRing();

// Add nodes to the ring
hashRing.AddNode("server1");
hashRing.AddNode("server2");
hashRing.AddNode("server3");

// Find which node should handle a key
string responsibleNode = hashRing.FindNodeFor("user123");
Console.WriteLine($"Key 'user123' maps to: {responsibleNode}");

// Remove a node - keys will be redistributed
hashRing.RemoveNode("server2");
```

## Limitations

This is a simplified implementation for learning purposes.
Possible enhancements:

- **Replication factor** for fault tolerance

