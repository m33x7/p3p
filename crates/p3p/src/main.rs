use std::collections::HashSet;
use std::net::SocketAddr;

mod transport;
mod bootstrap_discovery;

use crate::transport::{Transport};

fn main() -> std::io::Result<()> {

    let (transport, incoming_messages) = Transport::spawn_udp()?;

    let bootstrap_node: SocketAddr = "0.0.0.0:4000".parse().unwrap();

    let (listener_thread, discoverer_thread) = 
        bootstrap_discovery::BootstrapDiscoveryV1::bootstrap_discovery_start(
            transport,
            incoming_messages,
            HashSet::from([bootstrap_node])
        );

    listener_thread.join().unwrap();
    discoverer_thread.join().unwrap();
    
    Ok(())
}
