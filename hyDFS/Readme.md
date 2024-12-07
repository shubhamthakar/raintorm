# HyDFS Project Overview

## Table of Contents
- [Overview of Project](#overview-of-project)
- [Cluster Design and Architecture](#cluster-design-and-architecture)
- [Threads](#threads)
- [Replication Strategy](#replication-strategy)
- [Create and Get Operations](#create-and-get-operations)
- [Append Operations](#append-operations)
- [Merge Operation](#merge-operation)
- [Past MP Use](#past-mp-use)
- [How to Run](#how-to-run)
- [Directories](#directories)

## Overview of Project
HyDFS is a distributed file system designed for high availability, consistency, and fault tolerance. The system is implemented across a cluster of 10 nodes, with each node capable of functioning as a coordinator. Clients can connect to any coordinator node to perform key operations, such as `create`, `get`, `append`, and `merge`.

## Cluster Design and Architecture
- **Cluster Composition**: The cluster consists of 10 nodes, where any node can act as a coordinator.
- **Client Connectivity**: Clients can connect to any coordinator node for performing operations.

## Threads
- **Server Thread**: This thread listens for incoming TCP connections on a server socket and manages message exchanges over established connections. It leverages Python's `select.select` paradigm along with non-blocking TCP sockets for efficient communication.
- **Membership Monitoring Thread**: This thread monitors the membership list for node join, leave, or failure events and triggers replication processes when changes are detected.

## Replication Strategy
To ensure fault tolerance against up to 2 simultaneous node failures, HyDFS implements an active push-based replication strategy. Data is replicated across 3 replicas (1 primary and 2 secondary). The coordinator node performs the replication by writing data to all three replicas. In case of node failure detection, the first two predecessor nodes and the first successor node of the failed node are responsible for re-replicating their files using push-based replication, thus maintaining data availability and consistency.

## Create and Get Operations
- **Quorum Size**: The system operates with a quorum size of 2, ensuring overlapping between create and get quorums for data consistency and high availability.
- **Get Consistency**: In case of inconsistent append logs due to append failures, the coordinator returns the version with the largest append sequence number, representing the most recent data.
- **Acknowledgments**: The coordinator sends an acknowledgment to the client only after receiving confirmations from at least 2 replicas.

## Append Operations
Each replica maintains a unique append log for each client-file pair, with each append assigned a unique sequence number. The coordinator writes to replicas in a fixed order (e.g., replica 1 [primary], then replica 2 [secondary], etc.) to ensure consistent sequence numbers even with back-to-back client writes to different coordinators.

## Merge Operation
The merge process involves dual sorting for each file:
- **Sorting by Client**: Ensures that appends are ordered consistently by client across all replicas.
- **Sorting by Sequence Number**: Maintains the same order of appends from a single client across all replicas.

This dual sorting strategy guarantees that after merging, all replicas contain identical file contents, preserving data consistency across the cluster.

## Past MP Use
Previously, the MP2 fault detection was running on a different port. The membership list was used to detect failures, joins, and leaves, and replicate data accordingly.

## How to Run
1. **Scripts**: Use the scripts in the `scripts` directory to start and stop the program across all servers.
2. **Main Code**: The primary implementation of HyDFS is located in `hyDFS.py`.
3. **Client Simulation**: The `client` directory contains scripts that simulate client operations (both sequential and concurrent). A separate client with caching capabilities is also provided.

## Directories
- **`filesystem`**: Stores the files and append logs on HyDFS.
- **`local`**: Contains local files intended for upload to HyDFS.
- **`logs`**: Includes separate log files for HyDFS operations and the membership protocol for fault detection.
