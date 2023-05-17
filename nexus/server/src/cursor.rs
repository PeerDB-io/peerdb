use std::collections::HashMap;

use peer_cursor::CursorModification;
use pt::peers::Peer;

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

    fn add_cursor(&mut self, name: String, peer: Box<Peer>) {
        self.cursors.insert(name, peer);
    }

    fn remove_cursor(&mut self, name: String) {
        self.cursors.remove(&name);
    }

    pub fn get_peer(&self, name: &str) -> Option<&Box<Peer>> {
        self.cursors.get(name)
    }

    pub fn handle_event(&mut self, peer: Box<Peer>, cursor_modification: CursorModification) {
        match cursor_modification {
            CursorModification::Created(name) => {
                self.add_cursor(name, peer);
            }
            CursorModification::Closed(names) => {
                for name in names {
                    self.remove_cursor(name);
                }
            }
        }
    }
}
