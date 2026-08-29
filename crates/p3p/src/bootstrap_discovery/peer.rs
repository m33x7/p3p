use std::{collections::HashMap, fmt, net::SocketAddr, ops::Index, sync::{Arc, RwLock}};

use rand::RngExt;
use tokio::time::{Instant, Duration};

use crate::bootstrap_discovery::DiscoveryError;

pub struct Peer {
    // When we start - we don't know the peer id of bootstrap nodes.
    // Will be fixed when we implement encryption so bootstrap nodes will get a fixed peer_id.
    peer_id: PeerId,

    mutable_state: RwLock<PeerMutableState>
}

struct PeerMutableState {
    addr: SocketAddr, // TODO - peer can have multiple addresses in different networks
    last_ping: Option<Instant>,
}

// TODO - use RwLock
impl Peer {
    pub fn new(peer_id: PeerId, addr: SocketAddr) -> Self {
        Peer { mutable_state: RwLock::new(PeerMutableState { addr, last_ping: None }), peer_id }
    }

    pub fn get_addr(&self) -> SocketAddr {
        let state = self.mutable_state.read().unwrap();
        state.addr
    }

    pub fn get_last_ping(&self) -> Option<Instant> {
        let state = self.mutable_state.read().unwrap();
        state.last_ping
    }

    // the protocol is trivially poisonable. Malicious node can easily:
    // 1. Can flood the network with gossip about nonexistent peers, wrong IP/port pairs
    // 2. Try to get everyone to connect to a victim's IP (address-spoofing DoS).
    // TODO - try fixing using "Ethereum node records" or "signed peer records" from libp2p.
    pub fn refresh_from_gossip(&self, gossip: Gossip){
        let mut state = self.mutable_state.write().unwrap();
        state.addr = gossip.addr;
    }

    pub fn refresh_from_ping(&self){
        let mut state = self.mutable_state.write().unwrap();
        state.last_ping = Some(Instant::now());
    }

    pub fn to_gossip(&self) -> Gossip {
        Gossip { peer_id: self.peer_id, addr: self.get_addr() }
    }
}

// To later change to some certificate
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerId(u32);

impl PeerId {
    pub fn new(id: u32) -> Self {
        PeerId(id)
    }

    pub fn generate() -> Self {
        let mut rng = rand::rng();
        PeerId::new(rng.random_range(1..u32::MAX))
    }

    pub fn from_string(s: &str) -> Result<Self, DiscoveryError> {
        let id: u32 = s.trim().parse()?; // -> DiscoveryError::InvalidIntNumber via #[from]
        Ok(PeerId(id))
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct Gossip {
    pub peer_id: PeerId,
    pub addr: SocketAddr,
}

// Internal mutability via RwLock.    
pub struct PeerList {
    pub tt_suspect: Duration, // time to call a peer "suspect"
    pub tt_faulty: Duration, // time to call a peer "faulty"

    peers: RwLock<HashMap<PeerId, Arc<Peer>>>,
}

impl PeerList {
    pub fn new(tt_suspect: Duration, tt_faulty: Duration) -> Self { Self { tt_faulty, tt_suspect, peers: RwLock::new(HashMap::new()) } }

    pub fn add_trusty_peer(&self, peer: Peer) {
        // We add it as last pinged peer
        peer.refresh_from_ping();

        let mut peers = self.peers.write().unwrap();
        peers.insert(peer.peer_id, Arc::new(peer));
    }

    pub fn get_or_create(&self, peer_id: PeerId, addr: SocketAddr) -> Arc<Peer>{
        let mut peers = self.peers.write().unwrap();
        peers.entry(peer_id).or_insert(Arc::new(Peer::new(peer_id, addr))).clone()
    }

    // TODO - research: What attack can be done here if some node starts sending itself as part of the gossip.
    pub fn get_trusty_peers(&self, peers_number: Option<usize>) -> Vec<Arc<Peer>> {
        let trusty_peers = self.peers.read().unwrap();
        let k = peers_number.unwrap_or(trusty_peers.len());
        trusty_peers
            .iter()
            .filter(|(_, peer)| peer.get_last_ping().is_some_and(|last_ping| last_ping.elapsed() < self.tt_suspect))
            .map(|(_, peer)| Arc::clone(peer))
            .take(k) // TODO - this should be random pick
            .collect()
    }

    pub fn get_suspects(&self) -> Vec<Arc<Peer>> {
        let suspects = self.peers.read().unwrap();
        suspects
            .iter()
            .filter(|(_, peer)| peer.get_last_ping().is_none_or(|last_ping| last_ping.elapsed() > self.tt_suspect))
            .map(|(_, peer)| Arc::clone(peer))
            .collect()
    }

    pub fn remove_faulty_peers(&self) {
        let mut faulty_nodes = self.peers.write().unwrap();
        faulty_nodes
            .retain(|peer_id, peer| !peer.get_last_ping().is_none_or(|last_ping| last_ping.elapsed() > self.tt_faulty));
    }
}