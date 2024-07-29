Peer to peer social network base on distributed peer-to-peer graph database with content stored on IPFS

GDB is based on [Cypher](https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf)

There are nodes for Users, Posts, Root servers. Relationships for Likes, Blocks

Updates broadcast over P2P similar to Bitorrent. Messages are Cypher queries signed.

`CREATE (n:Person {name: 'Andres', title: 'Developer'})`

Supported actions `CREATE`, `MERGE`, `SET`, `DELETE`.

Can also `SUBSCRIBE` to `MATCH`.

How to track changes?
* Each node has a hash of previous 50 hashes + current ID