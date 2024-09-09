use ahash::{HashMap, HashMapExt};
// use rand::Rng;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;

use crate::local_node::LocalNode;
use crate::msg::{CtlMsg, NodeId, RaftMsg};

const TICK_DURATION: Duration = Duration::from_millis(100);

pub struct Cluster {
    nodes: HashMap<NodeId, LocalNode>,
    ctl_requests: mpsc::Receiver<CtlMsg>,
    ctl_replies: mpsc::Sender<CtlMsg>,
    requests_rx: mpsc::Receiver<RaftMsg>,
}

impl Cluster {
    pub fn new(
        node_count: u32,
        ctl_requests: mpsc::Receiver<CtlMsg>,
        ctl_replies: mpsc::Sender<CtlMsg>,
    ) -> Self {
        let (requests_tx, requests_rx) = mpsc::channel(32);
        let mut nodes = HashMap::new();
        for id in 0..node_count {
            let id = id as usize;
            nodes.insert(id, LocalNode::new(node_count, id, requests_tx.clone()));
        }
        Self {
            nodes,
            ctl_requests,
            ctl_replies,
            requests_rx,
        }
    }

    pub async fn run(&mut self) {
        let mut tick = 0;
        // TODO: This outer loop is because of sleep pinning. Find a better way.
        'outer: loop {
            let sleep = time::sleep(TICK_DURATION);
            tokio::pin!(sleep);

            loop {
                tokio::select! {
                    Some(msg) = self.ctl_requests.recv() => {
                        if !self.process_ctl_msg(msg).await {
                            break 'outer;
                        }
                        println!("Received CtlMsg");
                    }
                    Some(msg) = self.requests_rx.recv() => {
                        if !self.process_raft_msg(msg).await {
                            break 'outer;
                        }
                        println!("Received Msg");
                    }
                    _ = &mut sleep => {
                        tick += 1;
                        println!("Tick {tick}!");
                        for n in self.nodes.values_mut() {
                            n.tick().await;
                        }
                        break;
                    }
                }
            }
        }
        // select! on messages from nodes and controller as well as next_tick timer
        // -- forward messages to the appropriate node
        // -- send control message replies back to controller

        // Events:
        // * a node times out and sends
        // * a leader sends a heartbeat
        // * cluster sends a control message (like kill or request_state)
        // * cluster receives a control message (from tester)
        // *
    }

    async fn process_raft_msg(&mut self, msg: RaftMsg) -> bool {
        use RaftMsg::*;
        match msg {
            RequestVote { candidate, .. } => {
                for (n_id, n) in &mut self.nodes {
                    if *n_id == candidate {
                        continue;
                    }
                    n.receive_message(msg.clone()).await;
                }
            }
            SendVote { dest, .. } => {
                self.nodes
                    .get_mut(&dest)
                    .unwrap()
                    .receive_message(msg)
                    .await;
            }
            AppendEntries { leader, .. } => {
                for (n_id, n) in &mut self.nodes {
                    if *n_id == leader {
                        continue;
                    }
                    n.receive_message(msg.clone()).await;
                }
            }
            OutdatedTerm { dest, .. } => {
                self.nodes
                    .get_mut(&dest)
                    .unwrap()
                    .receive_message(msg)
                    .await;
            }
        }
        true
    }

    // TODO: Randomize timing of sending messages to different nodes.
    async fn process_ctl_msg(&mut self, msg: CtlMsg) -> bool {
        use CtlMsg::*;
        // TODO: If target is dead, only forward a Start message.
        match msg {
            GetClusterState => {
                let mut nodes = Vec::new();
                for n in self.nodes.values_mut() {
                    if !(n.is_dead || n.is_disconnected) {
                        nodes.push(n.get_state());
                    }
                }
                // let is_leader = node.state.is_leader;
                // let is_candidate = node.state.votes.len() > 0;
                self.ctl_replies.send(SendClusterState { nodes }).await;
            }
            SendClusterState { .. } => {
                println!("Shouldn't receive this!");
            }
            Kill { dest } => {
                self.nodes.get_mut(&dest).unwrap().kill();
            }
            Start { dest } => {
                self.nodes.get_mut(&dest).unwrap().start();
            }
            Connect { dest } => {
                self.nodes.get_mut(&dest).unwrap().connect();
            }
            Disconnect { dest } => {
                self.nodes.get_mut(&dest).unwrap().disconnect();
            }
            Shutdown {} => return false,
        }
        true
    }
}

// Node receives and sends messages.
// Cluster
