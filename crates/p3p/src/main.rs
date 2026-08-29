use std::collections::HashSet;

mod transport;
mod bootstrap_discovery;

use crate::{bootstrap_discovery::{Peer, peer::PeerId}, transport::{BindingPortRange, UdpTransport}};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let port_range = BindingPortRange { start: 4000, end: 4050 };
    let transport = UdpTransport::bind(port_range).await.unwrap();

    let mut bootstrap_nodes = vec![];
    let binded_port = transport.socket.local_addr()?.port();

    let my_peer_id = if binded_port == 4000 {
        println!("Starting as a bootstrap node. Port {}", binded_port);

        PeerId::new(5322)
    } else {
        println!("Starting as a client node. Port {}", binded_port);
        bootstrap_nodes.push(Peer::new(PeerId::generate(), "0.0.0.0:4000".parse().unwrap()));
        
        PeerId::generate()
    };

    let settings = bootstrap_discovery::Settings {
        k: 3,
        payload_size: 10,
        t_gossip_ms: 20000,
        t_ping_ms: 5000,
        my_peer_id,
        tt_suspect_ms: 10000,
        tt_faulty_ms: 20000
    };

    bootstrap_discovery::BootstrapDiscoveryV1::bootstrap_discovery_start(settings, transport, bootstrap_nodes).await;
    
    Ok(())
}
