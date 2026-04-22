use std::{net::SocketAddr, sync::{Arc, Mutex, mpsc::Receiver}, thread::{self, JoinHandle}, time::{Duration, SystemTime}};
use thiserror::Error;
use std::collections::{HashSet, HashMap};
use rand::prelude::*;
use crate::transport::{Transport, TransportMessage, TransportTrait};

#[derive(Clone)]
struct Peer {
    addr_to_last_ping: HashMap<SocketAddr, Option<SystemTime>>,
    peer_id: u32,
}

impl Peer {
    fn from_id(peer_id: u32) -> Peer { Peer { addr_to_last_ping: HashMap::with_capacity(0), peer_id } }
    fn from_seen_addr(peer_id: u32, addr: SocketAddr) -> Peer { Peer { addr_to_last_ping: HashMap::from([(addr, Some(SystemTime::now()))]), peer_id } }

    fn from_gossip(gossip: PeerGossip) -> Peer {
        let addr_to_last_ping = gossip.addr.into_iter().map(|a| (a, None)).collect(); // We never pinged peer ourself
        Peer { addr_to_last_ping, peer_id: gossip.peer_id }
    }

    fn get_gossip(&self) -> PeerGossip {
        PeerGossip {
            peer_id: self.peer_id,
            addr: self.addr_to_last_ping.keys().copied().collect(),
        }
    }

    fn update_from_message(&mut self, message: IncomingDiscoveryMessage){ // We heard from peer directly, we can update it's addr
        let addr = message.from_peer_addr;
        let now = Some(SystemTime::now());
        self.addr_to_last_ping
            .entry(addr)
            .insert_entry(now);
    }

    fn update_from_gossip(&mut self, gossip: &PeerGossip){
        let PeerGossip { peer_id: _, addr } = gossip;
        for addr in addr.into_iter() {
            if let None = self.addr_to_last_ping.get(&addr) {
                self.addr_to_last_ping.insert(*addr, None); // A new address learned that we should ping
            }
        }
    }

    fn should_be_dropped(&self) -> bool { // The peer that has no known addresses should be dropped. First call "remove_old_addresses"
        self.addr_to_last_ping.is_empty()
    }

    fn is_trusty(&self) -> bool { // If we can share this peer with others
        self.addr_to_last_ping.values().any(|v| v.is_some())
    }

    fn get_addresses_to_ping(&self, older_than: SystemTime) -> HashSet<SocketAddr> {
        self.addr_to_last_ping
            .iter()
            .filter_map(|(&addr, last_ping)|
                last_ping
                    .is_none_or(|t| t < older_than)
                    .then_some(addr)
            )
            .collect()
    }

    fn get_address(&self) -> Option<SocketAddr> { // Returns the last seen address that can be used for gossiping
        self.addr_to_last_ping
            .iter()
            .filter_map(|(&addr, last_ping)| last_ping.map(|t| (addr, t)))
            .max_by_key(|(_, t)| *t)
            .map(|(addr, _)| addr)
    }

    fn refresh_addr(&mut self, addr: SocketAddr) {
        self.addr_to_last_ping
            .insert(addr, Some(SystemTime::now()));
    }

    fn get_old_addresses(&self, older_than: SystemTime) -> HashSet<SocketAddr> { // Get addresses that were not pinged for long
    self.addr_to_last_ping
        .iter()
        .filter_map(|(&addr, last_ping)|last_ping.is_none_or(|t| t < older_than).then_some(addr))
        .collect()
    }

    fn remove_old_addresses(&mut self, older_than: SystemTime) { // We pinged them and can't do much here
        self.addr_to_last_ping
            .retain(|_, last_ping| !last_ping.is_none_or(|t| t < older_than));
    }
}

struct PeerGossip {
    peer_id: u32,
    addr: HashSet<SocketAddr>,
}

impl PeerGossip { 
    fn from_peer_id(peer_id: u32) -> Self {
        PeerGossip { peer_id, addr: HashSet::with_capacity(0) }
    }
}

impl DiscoveryMessage {
    // Returns None if the message was not recognized. Can be covered by tests.
    pub fn from_string(s: &str) -> Result<DiscoveryMessage, DiscoveryError> {
        let cmd = s.split_whitespace()
            .next()
            .ok_or(DiscoveryError::InvalidDiscoveryMessage)?;

        if cmd.eq_ignore_ascii_case("PEERLIST") {
            Self::parse_peerlist(s)
        } else if cmd.eq_ignore_ascii_case("PEERLISTGET") {
            Self::parse_peerlist_get(s)
        } else if cmd.eq_ignore_ascii_case("GOSSIP") {
            Self::parse_gossip(s)
        } else if cmd.eq_ignore_ascii_case("GOSSIPGET") {
            Self::parse_gossip_get(s)
        } else if cmd.eq_ignore_ascii_case("PING") {
            Ok(Self::Ping)
        } else if cmd.eq_ignore_ascii_case("PONG") {
            Ok(Self::Pong)
        } else {
            Err(DiscoveryError::InvalidDiscoveryMessage)
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            DiscoveryMessage::Ping => "PING".into(),
            DiscoveryMessage::Pong => "PONG".into(),

            DiscoveryMessage::PeerList { peers } => {
                let ids = peers.iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("PEERLIST {}", ids)
            }

            DiscoveryMessage::PeerListGet { peers: known_peers } => {
                let ids = known_peers.iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("PEERLISTGET {}", ids)
            }

            DiscoveryMessage::PeerGossip { gossip } => {
                let addrs = gossip.addr.iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("GOSSIP {} {}", gossip.peer_id, addrs)
            }

            DiscoveryMessage::PeerGossipGet { gossip } => {
                format!("GOSSIPGET {}", gossip.peer_id)
            }
        }
    }

    // PEERLIST 3432 45333 {peer_id}
    fn parse_peerlist(s: &str) -> Result<DiscoveryMessage, DiscoveryError> {
        let mut parts = s.split_whitespace();
        parts.next(); // skip "PEERLIST"

        let peers = parts
            .map(|p| p.parse::<u32>())
            .collect::<Result<HashSet<_>, _>>()?;

        Ok(DiscoveryMessage::PeerList { peers })
    }

    // PEERLISTGET 3432 45333 {peer_id}
    fn parse_peerlist_get(s: &str) -> Result<DiscoveryMessage, DiscoveryError> {
        let mut parts = s.split_whitespace();
        parts.next(); // skip "PEERLISTGET"

        let peers = parts
            .map(|p| p.parse::<u32>())
            .collect::<Result<HashSet<_>, _>>()?;

        Ok(DiscoveryMessage::PeerListGet { peers })
    }

    fn parse_gossip(s: &str) -> Result<DiscoveryMessage, DiscoveryError> {
        let mut parts = s.split_whitespace();

        parts.next(); // skip "GOSSIP"

        let peer_id = parts
            .next()
            .ok_or(DiscoveryError::InvalidDiscoveryMessage)?
            .parse::<u32>()?;

        let addr = parts
            .map(|p| p.parse::<SocketAddr>())
            .collect::<Result<HashSet<_>, _>>()?;

        Ok(DiscoveryMessage::PeerGossip {
            gossip: PeerGossip { peer_id, addr }
        })
    }

    fn parse_gossip_get(s: &str) -> Result<DiscoveryMessage, DiscoveryError> {
        let mut parts = s.split_whitespace();

        parts.next(); // skip "GOSSIPGET"

        let peer_id = parts
            .next()
            .ok_or(DiscoveryError::InvalidDiscoveryMessage)?
            .parse::<u32>()?;

        Ok(DiscoveryMessage::PeerGossipGet {
            gossip: PeerGossip::from_peer_id(peer_id)
        })
    }
}

impl fmt::Display for DiscoveryMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

#[derive(Debug, Error)]
enum DiscoveryError {
    #[error("Invalid discovery message.")]
    InvalidDiscoveryMessage,
    #[error("Invalid HELLO message")]
    InvalidHelloMessage,
    #[error("Invalid PEERLIST message")]
    InvalidPeerListMessage,
    #[error("Invalid int number")]
    InvalidIntNumber(#[from] std::num::ParseIntError),
    #[error("Invalid addr")]
    InvalidAddr(#[from] std::net::AddrParseError),
    #[error("Transport layer problem")]
    TransportError(#[from] std::io::Error)
}

pub struct BootstrapDiscoveryV1 {
    peers: Arc<Mutex<HashMap<u32, Peer>>>,
    transport: Transport,
    pub my_peer_id: u32,
}

struct IncomingDiscoveryMessage {
    from_peer_id: u32,
    from_peer_addr: SocketAddr, // With already valid listening port that we got from "OutgoingDiscoveryMessage"
    my_perceived_addr: SocketAddr,
    message: DiscoveryMessage,
}

// FROM 23434 PORT 8080 TO 167.12.19.4:8081 PEERLIST 1234 4324
impl IncomingDiscoveryMessage {
    fn from_transportmessage(value: TransportMessage) -> Result<IncomingDiscoveryMessage, DiscoveryError> {
        let mut parts = value.msg.split_whitespace();

        // FROM
        match parts.next() {
            Some(cmd) if cmd.eq_ignore_ascii_case("FROM") => {}
            _ => return Err(DiscoveryError::InvalidDiscoveryMessage),
        }

        let from_peer_id = parts
            .next()
            .ok_or(DiscoveryError::InvalidDiscoveryMessage)?
            .parse::<u32>()?;

        // PORT
        match parts.next() {
            Some(cmd) if cmd.eq_ignore_ascii_case("PORT") => {}
            _ => return Err(DiscoveryError::InvalidDiscoveryMessage),
        }

        let port = parts
            .next()
            .ok_or(DiscoveryError::InvalidDiscoveryMessage)?
            .parse::<u16>()
            .map_err(|_| DiscoveryError::InvalidDiscoveryMessage)?;

        // TO
        match parts.next() {
            Some(cmd) if cmd.eq_ignore_ascii_case("TO") => {}
            _ => return Err(DiscoveryError::InvalidDiscoveryMessage),
        }

        let my_perceived_addr = parts
            .next()
            .ok_or(DiscoveryError::InvalidDiscoveryMessage)?
            .parse::<SocketAddr>()?;

        // rest is discovery message
        let rest = parts.collect::<Vec<_>>().join(" ");
        let message = DiscoveryMessage::from_string(&rest)?;

        let mut from_peer_addr = value.addr;
        from_peer_addr.set_port(port);

        Ok(IncomingDiscoveryMessage {
            from_peer_id,
            from_peer_addr,
            my_perceived_addr,
            message,
        })
    }
}

use std::fmt;

impl fmt::Display for IncomingDiscoveryMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "INCOMING from_peer_id={} from_peer_addr={} perceived_addr={} message={:?}",
            self.from_peer_id,
            self.from_peer_addr,
            self.my_perceived_addr,
            self.message.to_string()
        )
    }
}

struct OutgoingDiscoveryMessage {
    to: SocketAddr,
    my_listening_port: u16, // So the receiver can replace my_perceived_addr with port and send a message back to us
    from_peer_id: u32,
    message: DiscoveryMessage,
}

impl OutgoingDiscoveryMessage {
    pub fn into_transport(self) -> TransportMessage {
        TransportMessage {
            addr: self.to,
            msg: format!(
                "FROM {} PORT {} TO {} {}",
                self.from_peer_id,
                self.my_listening_port,
                self.to,
                self.message.to_string()
            ),
        }
    }
}

impl fmt::Display for OutgoingDiscoveryMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OUTGOING to={} from_peer_id={} port={} message={:?}",
            self.to,
            self.from_peer_id,
            self.my_listening_port,
            self.message.to_string()
        )
    }
}

enum DiscoveryMessage {
    Ping,
    Pong,
    PeerList { peers: HashSet<u32> },
    PeerListGet { peers: HashSet<u32> },
    PeerGossip { gossip: PeerGossip },
    PeerGossipGet { gossip: PeerGossip },
}

impl BootstrapDiscoveryV1 {
    // TODO - add check for max message length
    // TODO - change receiver to be part of the Transport trait
    pub fn bootstrap_discovery_start(
        transport: Transport,
        incoming_messages: Receiver<TransportMessage>,
        bootstrap_nodes: HashSet<SocketAddr>) -> (JoinHandle<()>, JoinHandle<()>) {
        let mut rng = rand::rng();
        let my_peer_id = rng.random_range(1..u32::MAX);

        let peers = Arc::new(Mutex::new(HashMap::from([(my_peer_id, Peer::from_id(my_peer_id))])));
        let bootstrap_discovery_v1 = Arc::new(BootstrapDiscoveryV1 { peers, transport, my_peer_id });

        let listener_thread = thread::spawn({
            let bootstrap_discovery_v1 = Arc::clone(&bootstrap_discovery_v1);
            move || {
                loop {
                    let message = incoming_messages.recv().unwrap();
                    if let Err(e) = Self::handle_message(&bootstrap_discovery_v1, message.into()) {
                        println!("[BootstrapDiscoveryV1] Error in listening loop: {}", e);
                    }
                }
            }
        });

        // Ping bootstrap nodes after we started listener thread.
        // We can't add them right to the list of peers because we don't know their peer_id.
        // If I'm bootstrap node - messages are filtered out in "handle_message".
        for addr in bootstrap_nodes.into_iter() {
            bootstrap_discovery_v1.send(DiscoveryMessage::Ping, addr); // TODO - handle error.
        }

        let discoverer_thread = thread::spawn({
            let bootstrap_discovery_v1 = Arc::clone(&bootstrap_discovery_v1);
            move || {
                let mut rng = rand::rng();
                loop {
                    let older_than = SystemTime::now() - Duration::from_secs(30);

                    // Send PING to all long seen peers and addresses.
                    let pinged_peers = bootstrap_discovery_v1.ping_peers(older_than);

                    // Wait to receive PONG.
                    thread::sleep(Duration::from_secs(10));
                    
                    // After ping - we can cleanup.
                    let survived_pruning = bootstrap_discovery_v1.prune_peers(&pinged_peers, older_than);

                    // Gossip
                    bootstrap_discovery_v1.gossip(survived_pruning);

                    // Sleep for random interval.
                    thread::sleep(Duration::from_secs(rng.random_range(1..5))); 
                }
            }
        });

        (listener_thread, discoverer_thread)
    }

    fn ping_peers(&self, older_than: SystemTime) -> HashSet<u32> {
        let mut pinged_peers_ids = self.get_all_peer_ids(); // Snapshot. So we don't remove peers added while we were pinging
        pinged_peers_ids.remove(&self.my_peer_id); // Don't ping myself.

        // Send PING to all old addresses.
        let old_addresses = self.get_all_old_addresses(&pinged_peers_ids, older_than);
        for addr in old_addresses.into_iter() {
            self.send(DiscoveryMessage::Ping, addr); // TODO - handle error and remove address
        }

        pinged_peers_ids
    }

    fn gossip(&self, peer_ids: HashSet<u32>) {
        // Send PeerList for gossiping.
        let trusty_peers = self.get_trusty_peers();
        for addr in self.get_peer_addresses(&peer_ids) {
            self.send(DiscoveryMessage::PeerListGet { peers: trusty_peers.clone() }, addr);
        }
    }

    fn handle_message(&self, msg: TransportMessage) -> Result<(), DiscoveryError> {
        let incoming = IncomingDiscoveryMessage::from_transportmessage(msg)?;
        
        println!("[BootstrapDiscoveryV1] Incoming message: {}", incoming);

        let IncomingDiscoveryMessage { from_peer_id, from_peer_addr, my_perceived_addr, message} = incoming;

        // Don't react to messages from itself
        if from_peer_id == self.my_peer_id {
            return Ok(());
        }

        self.refresh_seen_peer(from_peer_id, from_peer_addr); // Refresh peer.
        self.refresh_seen_peer(self.my_peer_id, my_perceived_addr); // Also refresh myself for tracing.

        let mut responses: Vec<DiscoveryMessage> = Vec::new(); // Responses to send.
        match message {
            DiscoveryMessage::Ping => responses.push(DiscoveryMessage::Pong),
            DiscoveryMessage::Pong => {},
            DiscoveryMessage::PeerList { peers } => { // Add peers and request info for every new peer
                self.add_peers(peers).into_iter().map(
                    |new_peer| responses.push(DiscoveryMessage::PeerGossipGet { gossip: PeerGossip::from_peer_id(new_peer) }));
            },
            DiscoveryMessage::PeerListGet { peers } => { // Add peers and request info for every new peer
                self.add_peers(peers).into_iter().map(
                    |new_peer| responses.push(DiscoveryMessage::PeerGossipGet { gossip: PeerGossip::from_peer_id(new_peer) }));

                // Also add response for the list of peers that we know.
                responses.push(DiscoveryMessage::PeerList { peers: self.get_trusty_peers() });
            }
            DiscoveryMessage::PeerGossip { gossip } => {
                let _ = self.refresh_peer_from_gossip(gossip);
            },
            DiscoveryMessage::PeerGossipGet { gossip } => {
                let updated_peer_gossip = self.refresh_peer_from_gossip(gossip);

                // Also add response for the know info about the peer
                responses.push(DiscoveryMessage::PeerGossip { gossip: updated_peer_gossip });
            }
        }

        // Send all responses
        for message in responses {
            let outgoing = OutgoingDiscoveryMessage
            {
                my_listening_port: self.transport.get_binding_port(),
                to: from_peer_addr,
                from_peer_id: self.my_peer_id,
                message,
            };

            println!("[BootstrapDiscoveryV1] Outgoing message: {}", outgoing);

            // Continue even if any of them fails.
            if let Err(e) = self.transport.send(outgoing.into_transport()) {
                println!("[Boostrap discovery]: Error in transport: {}", e);
            }
        }

        Ok(())
    }

    fn add_peers(&self, gossip_peers: HashSet<u32>) -> HashSet<u32> { // Returns the list of peers that we don't know
        let mut peers = self.peers.lock().unwrap();
        let new_peers: HashSet<u32> = gossip_peers.into_iter().filter(|gossip_peer_id| peers.contains_key(gossip_peer_id)).collect();

        for &new_peer_id in new_peers.iter(){
            peers.insert(new_peer_id, Peer::from_id(new_peer_id));
        }
        
        new_peers
    }

    fn refresh_peer_from_gossip(&self, gossip: PeerGossip) -> PeerGossip {
        let peer_id: u32 = gossip.peer_id;

        let mut peers = self.peers.lock().unwrap();
        peers
            .entry(peer_id)
            .and_modify(|p| p.update_from_gossip(&gossip))
            .or_insert(Peer::from_gossip(gossip));

        peers.get(&peer_id).unwrap().get_gossip()
    }

    fn refresh_seen_peer(&self, peer_id: u32, addr: SocketAddr) {
        let mut peers = self.peers.lock().unwrap();
        peers
            .entry(peer_id)
            .and_modify(|p| p.refresh_addr(addr))
            .or_insert(Peer::from_seen_addr(peer_id, addr));
    }

    fn get_all_old_addresses(&self, peer_ids: &HashSet<u32>, addr_older_than: SystemTime) -> HashSet<SocketAddr> { // Returns peer ids of pinged peers so we can remove if some of them failed
        let peers = self.peers.lock().unwrap();
        peers
                .values()
                .filter(|peer| peer_ids.contains(&peer.peer_id))
                .flat_map(|peer| peer.get_old_addresses(addr_older_than))
                .collect()
    }

    fn get_all_peer_ids(&self) -> HashSet<u32> {
        self.peers.lock().unwrap().keys().copied().collect()
    }

    fn get_peer_addresses(&self, peer_ids: &HashSet<u32>) -> HashSet<SocketAddr> {
        let peers = self.peers.lock().unwrap();
        peers
            .values()
            .filter(|peer| peer_ids.contains(&peer.peer_id))
            .map(|peer| peer.get_address())
            .flatten()
            .collect()
    }

    fn get_trusty_peers(&self) -> HashSet<u32> {
        let peers = self.peers.lock().unwrap();
        peers        
            .values()
            .filter(|peer| peer.is_trusty() || peer.peer_id == self.my_peer_id) // Include myself.
            .map(|peer| peer.peer_id)
            .collect()
    }

    fn prune_peers(&self, peer_ids: &HashSet<u32>, addr_older_than: SystemTime) -> HashSet<u32> { // Returns survived pruning peers
        // Remove peers that we pinged. Clean all addresses only for peers that we pinged.
        let mut peers = self.peers.lock().unwrap();
        peers.retain(|peer_id, peer| {
            if peer_ids.contains(peer_id) {
                peer.remove_old_addresses(addr_older_than);
                return peer.should_be_dropped();
            } else {
                return true;
            }
        });

        peers.keys().filter(|peer_id| peer_ids.contains(peer_id)).copied().collect()
    }

    fn send(&self, message: DiscoveryMessage, to: SocketAddr) -> std::io::Result<()> {
        self.transport.send(OutgoingDiscoveryMessage { to, my_listening_port: self.transport.get_binding_port(), from_peer_id: self.my_peer_id, message }.into_transport())
    }
}

