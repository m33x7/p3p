// TODO - implement a connection pool that has "interior mutability"

use std::collections::{HashMap};
use std::sync::{Arc, Mutex};
use std::net::{SocketAddr, TcpStream};
use std::io;

use crate::transport::connection::{ConnectionFactory, TcpConnection};

pub struct ConnectionPool {
    connections: Arc<Mutex<HashMap<SocketAddr, TcpConnection>>>,
    pub connection_factory: ConnectionFactory,
}

impl ConnectionPool {
    pub fn new(connection_factory: ConnectionFactory) -> Arc<ConnectionPool> {
        let connections = Arc::new(Mutex::new(HashMap::new()));
        Arc::new(ConnectionPool { connections, connection_factory })
    }

    // [Dispatcher] uses this method to create and use new connections. It's ok if two threads request for a connection to the same addr.
    pub fn get(self: &Arc<ConnectionPool>, addr: SocketAddr) -> io::Result<PooledConnection> {
        {
            let mut connections = self.connections.lock().unwrap();

            if let Some(connection) = connections.remove(&addr) {
                println!("[ConnectionPool] Connection exists: {:?}", addr);
                return Ok(PooledConnection::new(connection, self.clone()));
            }
        } // Drop MutexGuard so we don't wait while connecting to the peer.

        let stream = TcpStream::connect(addr)?;
        let connection = self.connection_factory.new_connection_with_stream(stream, self.clone(), addr)?;
        println!("[ConnectionPool] Added connection: {:?}", addr);

        Ok(PooledConnection::new(connection, self.clone()))
    }

    // [Listener] uses this method.
    pub fn replace(self: Arc<ConnectionPool>, addr: SocketAddr, stream: TcpStream) -> io::Result<()> {
        let mut connections = self.connections.lock().unwrap();

        if let Some(removed) = connections.remove(&addr){
            println!("[ConnectionPool] Removed connection: {:?}", addr);
            removed.cancel();
        }

        let connection = self.connection_factory.new_connection_with_stream(stream, self.clone(), addr)?; // TODO - it's not nice that we block the whole connection pool before one connection is created. Easy to fix.
        connections.insert(addr, connection);

        println!("[ConnectionPool] Added connection: {:?}", addr);

        Ok(())
    }

    // [TcpConnection] used it to remove itself from a pool in case it was canceled
    pub fn remove(&self, addr: &SocketAddr) -> Option<TcpConnection> {
        let mut connections = self.connections.lock().unwrap();
        if let Some(removed) = connections.remove(addr){
            println!("Removed connection: {:?}", addr);
            removed.cancel();
        }

        None
    }

    pub fn return_connection(&self, addr: SocketAddr, connection: TcpConnection){
        let mut connections = self.connections.lock().unwrap();

        // If a newer connection was created - use newer connection instead
        if !connections.contains_key(&addr){
            connections.insert(addr, connection);
        }
    }
}

// Follows RAII pattern
pub struct PooledConnection {
    pub connection: Option<TcpConnection>,
    pool: Arc<ConnectionPool> // Pool to return to.
}

impl PooledConnection {
    pub fn new(connection: TcpConnection, pool: Arc<ConnectionPool>) -> PooledConnection {
        PooledConnection { connection: Some(connection) , pool }
    }

    pub fn send(&mut self, msg: &str) -> io::Result<()> {
        let mut connection = self.connection.take().expect("internal invariant");
        connection.send(msg)?;
        self.connection = Some(connection);
        Ok(())
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.connection.take() {
            self.pool.return_connection(conn.addr.clone(), conn);
        }
    }
}