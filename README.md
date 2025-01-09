### RainStorm: Stream Processing Framework

# Overview
RainStorm is a fault-tolerant stream-processing framework inspired by systems like Storm and Spark Streaming. It processes real-time data streams with exactly-once semantics, leveraging distributed group membership, a hybrid distributed file system (HyDFS), and Pythons asyncio for efficient task scheduling and execution.

# Architecture
Leader-Worker Design
Leader:
- Manages job submissions, task assignments, and fault recovery. Handles rescheduling tasks on worker failures.

Workers:
- Execute user-defined tasks such as transformations, filtering, and aggregations.
- Use HyDFS for persistent storage and state management.
- Implement exactly-once semantics with duplicate detection and state recovery.

# Features
Core Capabilities
Fault Tolerance:
- Dynamic task reassignment on failures.
- Persistent state and tuple logs in HyDFS for recovery.
- Consistent outputs under failure scenarios.

Exactly-once Semantics:
- Ensures each input record is processed exactly once, even during retries.
- Deduplication mechanisms with persistent logs.

Parallel Processing:

- Distributed execution of tasks across multiple stages.
- Hash partitioning for balanced workload distribution.
- Operations
- Transform: Applies user-defined functions to records.
- FilteredTransform: Filters records based on conditions, then transforms them.
- AggregateByKey: Maintains and updates aggregate statistics.


### HyDFS: Hybrid Distributed File System
HyDFS is a fault-tolerant, distributed file system combining features from HDFS and Cassandra, optimized for performance and reliability in distributed systems. It serves as the backbone of RainStorm, providing persistent storage for state and data.

# Key Features
Distributed Architecture
- Nodes are organized in a consistent hashing ring for efficient file location and distribution.
- Files are replicated across multiple nodes to ensure fault tolerance and high availability.

Fault Tolerance
- Supports up to two simultaneous node failures while maintaining data availability.
Active Push-based Replication:
- Each file is replicated to the first three successor nodes on the ring.
Automatic re-replication triggered on node failures to restore redundancy.

File Operations
- Create: Stores files in HyDFS, ensuring initial replication.
- Get: Fetches files with quorum-based consistency checks.
- Append: Allows data to be added to existing files while maintaining consistency across replicas.
- Merge: Reconciles inconsistencies between replicas, ensuring all nodes have identical data.

Consistency Guarantees
- Ensures eventual consistency:
Appends are applied in the same order across replicas.
Reads reflect the most recent appends by the same client.
Quorum-based operations for both read (get) and write (append).

Client-Side Caching
- Implements LRU caching for frequently accessed files to reduce latency.
- Cache size can be configured (e.g., 10%, 50%, 100% of data size).
- Supports performance optimization for workloads with Zipfian access patterns.

## Experiments on HyDFS

![image](https://github.com/user-attachments/assets/d424814c-2f6b-4bd3-b4aa-ade157f3c02d)

 Fig 1. Replication time reported here includes both the failure detection time (Ping Ack) and time to replicate. The larger file sizes (1 MB, 10 MB, and 20 MB) show a noticeable increase in replication time, with 20 MB having the longest time and the largest standard deviation, indicating more variability in replication time for larger files.

![image](https://github.com/user-attachments/assets/af38bd84-6900-4e64-b2cb-87992ce7a2d6)


Fig 2. For this experiment we measured the average bandwidth of nodes participating in file exchanges during replication and are reporting this value. We monitored the interface at each of these nodes to get packets sent and received per second and used that to calculate the bandwidth. The graph shows that bandwidth utilization increases as file size increases, with larger files (10 MB and 20 MB) using significantly more bandwidth compared to smaller ones.

![image](https://github.com/user-attachments/assets/dfff0202-9df2-44f7-968c-09783c88836b)


Fig 3. For a single client we did 1000 back-to-back appends, for 2 clients we did 500 back-to-back appends each and so on. As the number of concurrent clients grow merge time increases but the increase is not a lot because of the way we are handling merge and appends

![image](https://github.com/user-attachments/assets/9f1700a6-29f8-4257-acb4-48b16c623f7d)


Fig 4. Merge times for 40 KB appends are consistently higher than for 4 KB, and the increase with more concurrent clients is more pronounced, indicating larger data sizes lead to longer and more variable merge times.

![image](https://github.com/user-attachments/assets/d28bcae8-20fb-4d79-adbc-99a8b2a4b9fc)


Fig 5. For this experiment we wrote 1000 files to hyDFS each of size 4kb and performed 2000 sequential gets. The graph shows that as cache size increases, the overall get time decreases, improving performance. The difference between 50% and 100% cache size is small because, at 50%, the cache already holds most frequently accessed data, so increasing it to 100% offers minimal additional benefit.

![image](https://github.com/user-attachments/assets/524f598b-bffa-4ba1-b6c5-f50ace31a5fd)

 
 Fig 6. The Zipfian graph shows a sharp drop in get time from 0% to 10% cache size, with minimal improvement beyond that. This reflects the benefit of caching the most frequently accessed data early on. In contrast to this the uniform distribution has a steady improvement across all cache sizes.

![image](https://github.com/user-attachments/assets/39fc34b8-cf8b-4dc6-9764-299b3c395d5d)


Fig 7. This graph compares cache performance for uniform and Zipfian distributions at 0% and 50% cache sizes. For this experiment we wrote 1000 files to hyDFS each of size 4kb. Our query workload had 1800 gets and 200 appends. At 0% cache, both distributions have similar get times, showing no cache benefit. However, at 50% cache, the Zipfian distribution shows a significantly lower get time compared to the uniform distribution, indicating that caching benefits the Zipfian pattern more due to its skewed data access
