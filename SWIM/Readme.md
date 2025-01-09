# SWIM Protocol Implementation

## Overview

This directory contains the code for implementing the SWIM protocol for membership and fault detection in a distributed system. The SWIM protocol is designed to work efficiently with a large number of nodes, providing scalable and reliable membership management with low overhead.

### The implementation supports:
- **Socket-based communication using UDP**
- **Dissemination of membership lists via piggybacking on ping and ack messages**
- **Suspicion-based fault detection and failure recovery**
- **Utility scripts for bandwidth monitoring, mode switching, and calculating false positive rates**

---

## Files and Usage

### 1. process.py
This is the main implementation of the SWIM protocol. It runs the SWIM algorithm and manages communication between nodes.

**Usage:**

To run this process, use the following command:

```bash
python process.py```

To run processes on all machines, use the following command:

```bash
sh /home/chaskar/swim_runner_all.sh```

### 2. bandwidth.py
This script measures bandwidth usage at the network interface level, helping monitor the bandwidth overhead caused by the SWIM protocol.

**Usage:**

To run this process, use the following command:

```bash
python bandwidth.py```

### 3. utility.py
This file contains various utility functions that can be used to interact with the processes and manage the SWIM protocol.

**Usage:**

To run this process, use the following command:

List the membership lists of all processes:
```bash
python utility.py --command list_mem```

Switch between different operational modes:
```bash
python utility.py --command switch_modes```

Gracefully leave the cluster:
```bash
python utility.py --command leave```


### 4. process_with_ping_rec.py
This is an alternative implementation of the SWIM protocol that includes support for ping_req messages

