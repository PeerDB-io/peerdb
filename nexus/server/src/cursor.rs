use std::collections::HashMap;

use pt::peerdb_peers::Peer;

// PeerCursors is a map from name of cursor to the Peer that holds the cursor.
// This is used to route cursor events to the correct peer.
pub struct PeerCursors {
    cursors: HashMap<String, Box<Peer>>,
}

// have methods to deal with CursorModification events.
impl PeerCursors {
    pub fn new() -> Self {
        Self {
            cursors: HashMap::new(),
        }
    }

    pub fn add_cursor(&mut self, name: String, peer: Box<Peer>) {
        self.cursors.insert(name, peer);
    }

    pub fn remove_cursor(&mut self, name: String) {
        self.cursors.remove(&name);
    }

    pub fn get_peer(&self, name: &str) -> Option<&Box<Peer>> {
        self.cursors.get(name)
    }
}
