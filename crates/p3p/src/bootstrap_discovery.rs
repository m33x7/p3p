use std::{net::SocketAddr, sync::Arc};
use thiserror::Error;
use rand::prelude::*;
use crate::{bootstrap_discovery::{discovery_message::DiscoveryMessage, peer::PeerId}, transport::{Transport, TransportMessage}};

use tokio::time::{self, *};

pub mod peer;
pub use peer::Peer;
pub use peer::PeerList;

pub mod discovery_message;

#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("Invalid discovery message.")]
    InvalidDiscoveryMessage,
    #[error("Invalid int number")]
    InvalidIntNumber(#[from] std::num::ParseIntError),
    #[error("Invalid addr")]
    InvalidAddr(#[from] std::net::AddrParseError),
    #[error("Transport layer problem")]
    TransportError(#[from] std::io::Error)
}

pub struct BootstrapDiscoveryV1<T> where T: Transport + Send + Sync {
    transport: T,
    peers: PeerList,
    pub settings: Settings,
}

pub struct Settings {
    pub k: usize, // fanout - number of peers (as recipients) that we sent a gossip to. 3-5 is good enough for convergence.
    pub payload_size: usize, // number of peers (as information size) that we send within a gossip. Not used. We can use "rumor mongering" or pick random subset.
    pub t_gossip_ms: u64, // time between gossip rounds. We can't gossip faster than RTT.
    pub t_ping_ms: u64, // time between pings of peers.
    pub my_peer_id: PeerId,
    pub tt_suspect_ms: u64, // time since the latest ping to start suspecting the peer
    pub tt_faulty_ms: u64, // time since the latest ping to prune peer
}

impl<T> BootstrapDiscoveryV1<T> where T: Transport + Send + Sync {
    // TODO - add check for max message length
    // TODO - change receiver to be part of the Transport trait
    pub async fn bootstrap_discovery_start(settings: Settings, transport: T, bootstrap_nodes: Vec<Peer>) where T: 'static {

        // TODO - should be aquired from config.
        let peers = PeerList::new(Duration::from_millis(settings.tt_suspect_ms), Duration::from_millis(settings.tt_suspect_ms));

        // Current node doesn't add itself to the collection of peers. We start with the collection of bootstrap nodes.
        for bootstrap_node in bootstrap_nodes.into_iter() {
            peers.add_trusty_peer(bootstrap_node);
        }
        
        let mut gossip_tick = time::interval(time::Duration::from_millis(settings.t_gossip_ms));
        let mut ping_tick = time::interval(time::Duration::from_millis(settings.t_ping_ms));

        let bootstrap_discovery = Arc::new(BootstrapDiscoveryV1 { transport, peers, settings });
        
        let gossip = tokio::spawn({
            let bootstrap_discovery = Arc::clone(&bootstrap_discovery);
            async move {
                loop {
                    gossip_tick.tick().await;
                    bootstrap_discovery.gossip().await;
                }
            }
        });

        let listener = tokio::spawn({
            let bootstrap_discovery = Arc::clone(&bootstrap_discovery);
            async move {
                let mut buf = [0u8; 1024];
                loop {
                    bootstrap_discovery.receive_message(&mut buf).await;
                }
            }
        });

        let ping = tokio::spawn({
            let bootstrap_discovery = Arc::clone(&bootstrap_discovery);
            async move {
                loop {
                    ping_tick.tick().await;
                    bootstrap_discovery.ping_peers().await;
                }
            }
        });

        /* 
        let prune = tokio::spawn({
            let bootstrap_discovery = Arc::clone(&bootstrap_discovery);
            async move {
                loop {
                    bootstrap_discovery.prune_peers().await;
                }
            }
        });
        */

        // TODO - add logging everywhere and below adequate error handling
        gossip.await;
        listener.await;
        ping.await;
    }

    async fn gossip(&self) -> () {
        let trusty_peers = self.peers.get_trusty_peers(Some(self.settings.payload_size));

        let gossip: Vec<peer::Gossip> = trusty_peers.iter().map(|p| p.to_gossip()).collect();
        let discovery_message = DiscoveryMessage::Gossip { from: self.settings.my_peer_id, gossips: gossip };
        
        let k_peers = self.peers.get_trusty_peers(Some(self.settings.k));

        println!("Sending gossip of payload {} to {} trusty peers", trusty_peers.len(), k_peers.len());
        for peer in k_peers {
            self.send_msg(peer.get_addr(), &discovery_message).await;
        }
    }

    async fn ping_peers(&self) {
        let suspects = self.peers.get_suspects();

        for suspect in suspects.into_iter(){
            self.send_msg(suspect.get_addr(), &DiscoveryMessage::Ping {from: self.settings.my_peer_id }).await;
        }
    }

    async fn prune_peers(&self) {
        self.peers.remove_faulty_peers();
    }

    async fn receive_message(&self, buf: &mut [u8; 1024]) {

        // Exclude message about me as gossip
        match self.transport.receive(buf).await {
            Ok(TransportMessage { addr, msg}) => {
                println!(">> {}", msg);
                match DiscoveryMessage::from_string(&msg) {
                    Ok(discovery_message) => {
                        match discovery_message {
                            DiscoveryMessage::Ping { from } => {
                                // So we don't send ping back
                                self.peers
                                    .get_or_create(from, addr)
                                    .refresh_from_ping();

                                self.send_msg(addr, &DiscoveryMessage::Pong { from: self.settings.my_peer_id }).await;
                            },
                            DiscoveryMessage::Pong { from } => {
                                self.peers
                                    .get_or_create(from, addr)
                                    .refresh_from_ping();
                            },
                            DiscoveryMessage::Gossip { from: _, gossips } => {
                                for gossip in gossips.into_iter() {
                                    if gossip.peer_id != self.settings.my_peer_id {
                                        self.peers
                                            .get_or_create(gossip.peer_id, gossip.addr)
                                            .refresh_from_gossip(gossip);
                                    }
                                }
                            }
                        }
                    },
                    Err(e) => eprintln!("Error when parsing incoming message {e}")
                }
            },
            Err(e) => eprintln!("Error when receiving a message {e}")
        }
    }

    async fn send_msg(&self, addr: SocketAddr, discovery_message: &DiscoveryMessage){
        let msg = discovery_message.to_string();
        println!("<< {}", msg);
        let transport_message = TransportMessage { addr, msg: discovery_message.to_string() };
        if let Err(e) = self.transport.send(transport_message).await {
            eprintln!("Error sending message. {e}");
        }
    }
}

