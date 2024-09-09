use tokio::sync::mpsc;

use crate::msg::{NodeId, RaftMsg, RaftNodeSummary};
use crate::raft_node::RaftNode;

/// Simulates a node on a network but run locally.
pub struct LocalNode {
    raft_node: RaftNode,
    requests_ch: mpsc::Sender<RaftMsg>,
    pub is_disconnected: bool,
    pub is_dead: bool,
}

impl LocalNode {
    pub fn new(node_count: u32, id: NodeId, requests_ch: mpsc::Sender<RaftMsg>) -> Self {
        Self {
            raft_node: RaftNode::new(node_count, id),
            requests_ch,
            is_disconnected: false,
            is_dead: false,
        }
    }

    pub async fn receive_message(&mut self, msg: RaftMsg) {
        if self.is_dead || self.is_disconnected {
            return;
        }
        let maybe_msg = self.raft_node.receive_message(msg);
        self.send_msg(maybe_msg).await;
    }

    pub async fn tick(&mut self) {
        if self.is_dead {
            return;
        }
        let maybe_msg = self.raft_node.tick();
        if !self.is_disconnected {
            self.send_msg(maybe_msg).await;
        }
    }

    async fn send_msg(&mut self, maybe_msg: Option<RaftMsg>) {
        if let Some(msg) = maybe_msg {
            println!("Sending message {:?}", msg);
            self.requests_ch.send(msg).await;
        }
    }

    pub fn get_state(&self) -> RaftNodeSummary {
        self.raft_node.get_state()
    }

    pub fn kill(&mut self) {
        self.is_dead = true;
    }

    pub fn start(&mut self) {
        self.raft_node.init();
    }

    pub fn connect(&mut self) {
        self.is_disconnected = false;
    }

    pub fn disconnect(&mut self) {
        self.is_disconnected = true;
    }
}
