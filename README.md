# Chain Replica Distributed System

A proof-of-concept distributed system based on the paper ["Chain Replication for Supporting High Throughput and Availability"](https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf) by Robbert Renesse and Fred Schneider. The client is written in Python by [Professor Benjerman Reed](https://www.sjsu.edu/people/ben.reed/) at San Jose State University. And the server is written in Rust by me, Alex Wolski. The system relies on ZooKeeper as the oracle.

# Requirements

1. [ZooKeeper](https://zookeeper.apache.org/releases.html) version 3.5 or later.
2. [Rust](https://www.rust-lang.org/tools/install) version 1.55.0 or later.
3. [Python](https://www.python.org/downloads/) version 3.9 or later.

# Usage

**Start the ZooKeeper Instance**
Navigate to the repo root directory and run:
`java -jar <ZooKeeper Jar File> server zk_config.cfg`

**Start a Replica Server**
Navigate to the Chain_Replica folder and run:
`cargo run <IP Address> <Root ZNode>`

**Start a Replica Client**
Navigate to the Chain_Client folder and run:
`python3 chain_client.py <IP Address> <Root ZNode> <Comamnd> <Args>`

For the list of commands, run:
`python3 chain_client.py --help`