use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::bootstrap_discovery::peer::{Gossip, PeerId};
use crate::bootstrap_discovery::DiscoveryError;

pub enum DiscoveryMessage {
    Ping { from: PeerId },
    Pong { from: PeerId },
    Gossip { from: PeerId, gossips: Vec<Gossip> },
}

impl DiscoveryMessage {
    /// Formats supported:
    ///   "PING FROM {peer_id}"
    ///   "PONG FROM {peer_id}"
    ///   "GOSSIP FROM {peer_id}. {peer_id}={addr}, {peer_id}={addr}, ..."
    pub fn from_string(s: &str) -> Result<DiscoveryMessage, DiscoveryError> {
        let mut parts = s.splitn(2, char::is_whitespace);
        let cmd = parts.next().ok_or(DiscoveryError::InvalidDiscoveryMessage)?;
        let rest = parts.next().unwrap_or("").trim();

        if cmd.eq_ignore_ascii_case("PING") {
            let from = Self::parse_from(rest)?;
            Ok(Self::Ping { from })
        } else if cmd.eq_ignore_ascii_case("PONG") {
            let from = Self::parse_from(rest)?;
            Ok(Self::Pong { from })
        } else if cmd.eq_ignore_ascii_case("GOSSIP") {
            // rest is: "FROM {peer_id}. {peer_id}={addr}, {peer_id}={addr}"
            let rest = rest
                .strip_prefix("FROM")
                .map(str::trim)
                .ok_or(DiscoveryError::InvalidDiscoveryMessage)?;

            let (from_part, gossip_part) = rest
                .split_once('.')
                .ok_or(DiscoveryError::InvalidDiscoveryMessage)?;

            let from = PeerId::from_string(from_part.trim())?;

            let gossip_part = gossip_part.trim();
            let gossips = if gossip_part.is_empty() {
                Vec::new()
            } else {
                gossip_part
                    .split(',')
                    .map(|entry| {
                        let entry = entry.trim();
                        let (id_str, addr_str) = entry
                            .split_once('=')
                            .ok_or(DiscoveryError::InvalidDiscoveryMessage)?;
                        let peer_id = PeerId::from_string(id_str.trim())?;
                        let addr: SocketAddr = addr_str.trim().parse()?; // -> InvalidAddr
                        Ok(Gossip { peer_id, addr })
                    })
                    .collect::<Result<Vec<_>, DiscoveryError>>()?
            };

            Ok(Self::Gossip { from, gossips })
        } else {
            Err(DiscoveryError::InvalidDiscoveryMessage)
        }
    }

    fn parse_from(rest: &str) -> Result<PeerId, DiscoveryError> {
        let id_str = rest
            .strip_prefix("FROM")
            .map(str::trim)
            .ok_or(DiscoveryError::InvalidDiscoveryMessage)?;
        PeerId::from_string(id_str)
    }
}

use std::fmt;

impl fmt::Display for DiscoveryMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DiscoveryMessage::Ping { from } => write!(f, "PING FROM {}", from),
            DiscoveryMessage::Pong { from } => write!(f, "PONG FROM {}", from),
            DiscoveryMessage::Gossip { from, gossips } => {
                write!(f, "GOSSIP FROM {}. ", from)?;
                for (i, g) in gossips.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}={}", g.peer_id, g.addr)?;
                }
                Ok(())
            }
        }
    }
}