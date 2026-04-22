# p3p 
This is the project where I try out and learn about P2P + Crypto + Math + Rust. Here, no AI agent is used on purpose - to become more fluent in Rust.

### TODO items:

crate p3p:
- [X] Two peers chat
- [X] Discovery protocol - some toy one first
- [X] Multiple incoming connections
- [X] Fix issue when listening/sending ports are different
- [X] Transport layer - connection pool with interior mutability
- [X] Transport layer - dispatcher
- [ ] Basic message encryption
- [X] Peer ID
- [ ] Kademlia
- [X] UDP
- [ ] QUIC
- [ ] Multiplexing
- [ ] Use self-written async runtime
- [ ] Use Tokio
- [ ] Connection TTL
- [ ] Inbound/outbound connection distinction
- [ ] NAT handling throw handshakes
- [ ] Another framing for TCP
- [ ] Add UT
- [ ] Noise encryption
- [ ] Add versioning to discoverability and lower levels

crate async_runtime:
- [ ] Implement async runtime (should be easy !!!)

overall project:
- [ ] Add some tests where necessary

