use tokio::sync::{mpsc, oneshot};

use rustraft_raft::{
    msg::{LocalNodeSummary, NodeId, RaftCmd, RaftMsg, RaftNodeSummary},
    RaftNode,
};

pub enum LocalNodeMsg {
    Cmd {
        cmd: RaftCmd,
    },
    Msg {
        msg: RaftMsg,
    },
    SummaryRequest {
        tx: oneshot::Sender<LocalNodeSummary>,
    },
    Kill,
    Start,
    Connect,
    Disconnect,
    Shutdown,
}

/// Simulates a node on a network but run locally.
pub struct LocalNode {
    pub id: NodeId,
    pub is_disconnected: bool,
    pub is_dead: bool,
    /// Channels for communicating with network
    incoming_rx: mpsc::Receiver<LocalNodeMsg>,
    outgoing_tx: mpsc::Sender<(NodeId, RaftMsg)>,
    /// Channels for communicating with Raft implementation
    raft_incoming_network_tx: mpsc::Sender<RaftMsg>,
    raft_outgoing_network_rx: mpsc::Receiver<(NodeId, RaftMsg)>,
    raft_client_tx: mpsc::Sender<RaftCmd>,
    /// Channel for testing
    raft_summary_request_tx: mpsc::Sender<oneshot::Sender<RaftNodeSummary>>,
}

impl LocalNode {
    pub fn new(
        node_count: u32,
        id: NodeId,
        incoming_rx: mpsc::Receiver<LocalNodeMsg>,
        outgoing_tx: mpsc::Sender<(NodeId, RaftMsg)>,
    ) -> Self {
        let (raft_incoming_network_tx, raft_incoming_network_rx) = mpsc::channel(1024);
        let (raft_outgoing_network_tx, raft_outgoing_network_rx) = mpsc::channel(1024);
        let (raft_client_tx, raft_client_rx) = mpsc::channel(1024);
        let (raft_summary_request_tx, raft_summary_request_rx) = mpsc::channel(1024);

        tokio::spawn(async move {
            let mut raft_node = RaftNode::new(
                node_count,
                id,
                raft_incoming_network_rx,
                raft_outgoing_network_tx,
                raft_client_rx,
                raft_summary_request_rx,
            );
            raft_node.run().await;
        });

        Self {
            id,
            is_disconnected: false,
            is_dead: false,
            incoming_rx,
            outgoing_tx,
            raft_incoming_network_tx,
            raft_outgoing_network_rx,
            raft_client_tx,
            raft_summary_request_tx,
        }
    }

    pub async fn run(&mut self) {
        loop {
            use LocalNodeMsg::*;
            tokio::select! {
                Some(m) = self.incoming_rx.recv() => {
                    match m {
                        Msg { msg } => {
                            self.receive_message(msg).await;
                        }
                        Cmd { cmd } => {
                            self.apply_cmd(cmd).await;
                        }
                        SummaryRequest { tx } => {
                            let (raft_tx, raft_rx) = oneshot::channel();
                            self.raft_summary_request_tx.send(raft_tx).await.unwrap();
                            let raft = raft_rx.await.unwrap();
                            tx.send(LocalNodeSummary {
                                id: self.id,
                                is_dead: self.is_dead,
                                is_disconnected: self.is_disconnected,
                                raft,
                            }).unwrap();
                        }
                        Kill => self.kill(),
                        Start => self.start(),
                        Disconnect => self.disconnect(),
                        Connect => self.connect(),
                        Shutdown => break,
                    }
                }
                Some((dest, msg)) = self.raft_outgoing_network_rx.recv() => {
                    if self.is_dead || self.is_disconnected {
                        continue
                    }
                    println!("{} to {}: Sending {:?}", self.id, dest, msg);
                    self.outgoing_tx.send((dest, msg)).await.unwrap();
                    println!("--{}: Sent!", self.id);
                }
            }
        }
    }

    async fn apply_cmd(&mut self, cmd: RaftCmd) {
        if self.is_dead {
            return;
        }
        self.raft_client_tx
            .send(cmd)
            .await
            .expect("Failed to forward RaftCmd");
    }

    async fn receive_message(&mut self, msg: RaftMsg) {
        if self.is_dead || self.is_disconnected {
            return;
        }
        self.raft_incoming_network_tx
            .send(msg)
            .await
            .expect("Failed to forward RaftMsg");
    }

    fn kill(&mut self) {
        self.is_dead = true;
    }

    fn start(&mut self) {
        if self.is_dead {
            // TODO: Kill running raft node task and create a new one
            // to simulate starting fresh.
            self.is_dead = false;
        }
    }

    fn connect(&mut self) {
        self.is_disconnected = false;
    }

    fn disconnect(&mut self) {
        self.is_disconnected = true;
    }
}
