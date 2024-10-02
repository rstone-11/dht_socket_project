## DHT Socket Project

### Overview

This project implements a **Distributed Hash Table (DHT)** using **UDP sockets** in Python. The DHT allows multiple peers to form a network, distribute data, and process queries using a hot-potato style protocol. The project is designed to simulate distributed systems by managing peers, their state, and interactions over the network in a ring topology. The main components of the project are:

- A **Manager (Server)** that handles peer registration, DHT setup, and query processing.
- A **Peer (Client)** that can join, leave, query, and manage DHT data.

This project is based on the **CSE 434** course at **ASU** and was completed by Ryan Stone and Jack Heckenlaible.

### Features

- **Peer Registration**: Peers can register with the manager and become part of the DHT network.
- **DHT Setup**: A ring topology is established, and peers receive unique identifiers.
- **Query Handling**: Peers can query the DHT for specific data, which is processed using a hot-potato protocol.
- **Peer Churn Management**: Peers can leave and join the DHT dynamically while ensuring the integrity of the network.
- **CSV Data Storage and Querying**: Each peer holds part of a distributed CSV dataset. Queries to the DHT retrieve specific rows from the CSV based on event IDs.
- **DHT Teardown**: The DHT can be torn down by the leader, freeing up resources and resetting peer states.

Peers have three states: `Free` (registered but not part of the DHT, can issue queries), `InDHT` (part of the DHT), and `Leader` (responsible for DHT setup and management).


### DHT Architecture

The system uses a **ring-based topology** for the DHT. Each peer maintains part of the distributed hash table (DHT) and can forward queries to other peers. The DHT uses a **hot-potato protocol** for query processing, where a query is forwarded randomly among peers until the requested data is found or all peers have been queried.

When the DHT is set up, each record from the storm event CSV files is assigned to a specific peer based on the **hash** of the record's `event_id`. The `event_id` is hashed to a position in a hash table, and this position is then mapped to a peer in the ring. When a peer receives a query, it checks if the record is stored at its location based on the `event_id`. If not, it forwards the query to another peer, and this process continues until the responsible peer is found. The peer that holds the correct record then returns the data to the querying peer.

### Getting Started
#### Project Files

- `server.py`: Implements the manager responsible for handling peer registration and DHT management.
- `client.py`: Implements the peer logic to communicate with the manager and other peers in the DHT.
- `data/`:  This folder contains CSV files of storm event data. Fields include the `event_id` (unique identifier for the event) and other details like `state`, `year`, and `event_type`.

#### Running the Manager (Server)
The manager process handles all peer registrations, DHT setup, queries, and peer states. To start the manager:

```bash
python3 server.py <port_number>
```

#### Running the Peer (Client)
The peer process can join the network, issue commands to the manager, and communicate with other peers. Peers are assigned ports between 7001 and 7499 for communication. To start a peer:

```bash
python3 client.py <manager_ip> <manager_port>
```

#### Commands
- `register <peer-name> <IPv4-address> <m-port> <p-port>`: Registers the peer with the manager. The peer specifies its manager port and peer communication port.

- `setup-dht <peer-name> <n> <YYYY>`: Sets up the DHT for a specified number of peers (`n`) using the storm event data from the year `YYYY`. The peer that sends the command becomes the leader and is responsible for completing the setup of the DHT. The CSV files for the years 1950-1952 and 1996 are included, and the records are distributed to peers based on a hash of their event ID.
**Note**: Value of `n` must be at least 3.
- `query-dht <peer-name>`: Searches the DHT for a random storm event record, starting with a random peer and forwarding the query until the correct peer is found. If the event ID is not found in the DHT, the message "Storm event <event_id> not found in the DHT" will be returned. **Note**: Only peers in the `Free` state can issue queries.
- `leave-dht <peer-name>`: Leaves the DHT, triggering a rebuild of the remaining peers. There must be at least 3 peers in the DHT at all times. 
- `join-dht <peer-name>`: Joins the DHT and integrates into the existing peer network.
- `teardown-dht <peer-name>`: Tears down the DHT, removing all records and freeing up the peers that were part of the DHT. Only the leader of the DHT can tear it down. 
- `deregister <peer-name>`: Removes the peer from the system if it is in a `Free` state.