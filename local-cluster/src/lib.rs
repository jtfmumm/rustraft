use ahash::{HashMap, HashMapExt};
use local_node::LocalNodeMsg;
use rustraft_raft::msg::Destinations;
use std::collections::VecDeque;
use tokio::{
    sync::{mpsc, oneshot},
    time::{self, Duration, Instant},
};

pub mod local_cluster_tester;
pub mod local_node;

use crate::local_node::LocalNode;
use rustraft_raft::msg::{CtlMsg, NodeId, RaftMsg};

pub struct Cluster {
    /// Channels for sending to individual nodes
    node_txs: HashMap<NodeId, mpsc::Sender<LocalNodeMsg>>,
    /// Channel for messages from cluster runner
    ctl_requests: mpsc::Receiver<CtlMsg>,
    /// Channel for sending messages to cluster runner
    ctl_replies: mpsc::Sender<CtlMsg>,
    /// Channel for messages from nodes
    outgoing_rx: mpsc::Receiver<(NodeId, RaftMsg)>,
    /// Pending messages to send.
    raft_msg_sending_queue: VecDeque<(NodeId, RaftMsg)>,
    ctl_msg_sending_queue: VecDeque<CtlMsg>,
}

impl Cluster {
    pub fn new(
        node_count: u32,
        ctl_requests: mpsc::Receiver<CtlMsg>,
        ctl_replies: mpsc::Sender<CtlMsg>,
    ) -> Self {
        let (outgoing_tx, outgoing_rx) = mpsc::channel(1024);
        let mut node_txs = HashMap::new();
        for id in 0..node_count {
            let (incoming_tx, incoming_rx) = mpsc::channel(1024);
            let id = id as usize;
            let tx = outgoing_tx.clone();
            tokio::spawn(async move {
                let mut node = LocalNode::new(node_count, id, incoming_rx, tx);
                node.run().await;
            });
            node_txs.insert(id, incoming_tx);
        }
        Self {
            node_txs,
            ctl_requests,
            ctl_replies,
            outgoing_rx,
            raft_msg_sending_queue: VecDeque::new(),
            ctl_msg_sending_queue: VecDeque::new(),
        }
    }

    pub async fn run(&mut self) {
        let mut next_timeout = Instant::now() + Duration::from_millis(10);
        loop {
            self.send_from_queues().await;

            let sleep = time::sleep_until(next_timeout);
            tokio::select! {
                Some(msg) = self.ctl_requests.recv() => {
                    println!("Cluster received CtlMsg {:?}", msg);
                    if matches!(msg, CtlMsg::Shutdown) {
                        println!("Shutting down...");
                        return self.shutdown().await;
                    }
                    self.ctl_msg_sending_queue.push_back(msg);
                }
                Some((dest, msg)) = self.outgoing_rx.recv() => {
                    println!("Cluster received Msg for {}: {:?}", dest, msg);
                    self.raft_msg_sending_queue.push_back((dest, msg));
                }
                _ = sleep => {}
            }
            next_timeout = Instant::now() + Duration::from_millis(10);
        }
    }

    /// Process messages sent from cluster runner
    async fn process_ctl_msg(&mut self, msg: CtlMsg) {
        use CtlMsg::*;
        match msg {
            SendCmd { dest, cmd } => {
                self.node_txs
                    .get_mut(&dest)
                    .unwrap()
                    .send(LocalNodeMsg::Cmd { cmd })
                    .await
                    .unwrap();
            }
            GetClusterState => {
                let mut nodes = Vec::new();
                for n_tx in self.node_txs.values_mut() {
                    let (tx, rx) = oneshot::channel();
                    n_tx.send(LocalNodeMsg::SummaryRequest { tx })
                        .await
                        .unwrap();
                    nodes.push(rx.await.unwrap());
                }
                self.ctl_replies
                    .send(SendClusterState { nodes })
                    .await
                    .expect("Failed to send SendClusterState");
            }
            SendClusterState { .. } => {
                eprintln!("Cluster shouldn't receive SendClusterState!");
            }
            Kill { dest } => {
                self.node_txs
                    .get_mut(&dest)
                    .unwrap()
                    .send(LocalNodeMsg::Kill)
                    .await
                    .unwrap();
            }
            Start { dest } => {
                self.node_txs
                    .get_mut(&dest)
                    .unwrap()
                    .send(LocalNodeMsg::Start)
                    .await
                    .unwrap();
            }
            Connect { dest } => {
                self.node_txs
                    .get_mut(&dest)
                    .unwrap()
                    .send(LocalNodeMsg::Connect)
                    .await
                    .unwrap();
            }
            Disconnect { dest } => {
                self.node_txs
                    .get_mut(&dest)
                    .unwrap()
                    .send(LocalNodeMsg::Disconnect)
                    .await
                    .unwrap();
            }
            Shutdown {} => {
                for n_tx in self.node_txs.values() {
                    n_tx.send(LocalNodeMsg::Shutdown).await.unwrap();
                }
                panic!("Shutdown should already have been handled!");
            }
        }
    }

    async fn send_msg_to_node(&mut self, dest: NodeId, msg: RaftMsg) {
        self.node_txs
            .get_mut(&dest)
            .unwrap()
            .send(LocalNodeMsg::Msg { msg })
            .await
            .expect("Failed to send RaftMsg to LocalNode");
    }

    /// If channels are not full, send messages to nodes.
    /// If we return false, it's time to shut down
    async fn send_from_queues(&mut self) {
        while !self.raft_msg_sending_queue.is_empty() {
            let (dest, _) = self.raft_msg_sending_queue.front().unwrap();
            if !self.can_send_to_node(*dest) {
                break;
            }
            let (dest, msg) = self.raft_msg_sending_queue.pop_front().unwrap();
            self.send_msg_to_node(dest, msg).await;
        }
        while !self.ctl_msg_sending_queue.is_empty() {
            let msg = self.ctl_msg_sending_queue.front().unwrap();
            if !self.can_send_ctl_msg(msg) {
                break;
            }
            let msg = self.ctl_msg_sending_queue.pop_front().unwrap();
            self.process_ctl_msg(msg).await;
        }
    }

    fn can_send_ctl_msg(&self, msg: &CtlMsg) -> bool {
        match msg.destinations() {
            Destinations::All => self.can_send_to_all_nodes(),
            Destinations::One(dest) => self.can_send_to_node(dest),
            Destinations::None => panic!("Shouldn't have queued a message with no destination!"),
        }
    }

    fn can_send_to_all_nodes(&self) -> bool {
        let mut can_send = true;
        for (node_id, _) in self.node_txs.iter() {
            if !self.can_send_to_node(*node_id) {
                can_send = false;
            }
        }
        can_send
    }

    fn can_send_to_node(&self, node_id: NodeId) -> bool {
        !self.node_txs.get(&node_id).unwrap().is_closed()
    }

    async fn shutdown(&mut self) {
        println!("Local cluster received Shutdown!");
        for n_tx in self.node_txs.values() {
            n_tx.send(LocalNodeMsg::Shutdown).await.unwrap();
        }
    }
}
