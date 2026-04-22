use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::TryRecvError;
use std::thread;
use std::io;
use std::time::Duration;

mod transport;
mod bootstrap_discovery;

use crate::transport::{TransportMessage, Transport, TransportTrait};

fn main() -> std::io::Result<()> {
    let (transport, incoming_messages) = Transport::spawn_udp()?;

    let bootstrap_node: SocketAddr = "0.0.0.0:4000".parse().unwrap();

    let (listener_thread, discoverer_thread) = 
        bootstrap_discovery::BootstrapDiscoveryV1::bootstrap_discovery_start(
            transport,
            incoming_messages,
            HashSet::from([bootstrap_node])
        );

    listener_thread.join();
    discoverer_thread.join();
    
    Ok(())
}
