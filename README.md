# Partitioned and Replicated KVS
## Introduction
This is a Django implementation of a partitioned and replicated Key-Value Store.
It uses [consistent hashing](https://en.wikipedia.org/wiki/Consistent_hashing) as its partitioning strategy and places groups of nodes around the hashing circle.

## Requirements
-Docker  
-Python 2.*

## Running
Look at [build.sh](build.sh) for examples on this. Build.sh opens six nodes in a tmux terminal and initializes four of them.
The initial group of partitions needs the VIEW environment variable with a list of the other initial partitions' addresses in order to initialize correctly. In addition, you need to supply the environment variable IPPORT with the node's address and port.

## Issues
Current the Key-Value store stops working of all nodes in a partition are down. This is because each partition only stores information about its successor group and predecessor group. The fix for this will be to either keep a full list of all nodes in the table in every node, or to implement [Chord hashing](https://en.wikipedia.org/wiki/Chord_(peer-to-peer))
