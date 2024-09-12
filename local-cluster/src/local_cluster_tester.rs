use color_eyre::eyre::{bail, eyre};
use rustraft_raft::msg::{CtlMsg, LocalNodeSummary, NodeId, RaftCmd};
use std::time::Duration;
use tokio::{sync::mpsc, time::sleep};

use crate::Cluster;

pub type Nodes = Vec<LocalNodeSummary>;

pub struct LocalClusterRunner {
    node_count: u32,
    tx: mpsc::Sender<CtlMsg>,
    rx: mpsc::Receiver<CtlMsg>,
}

impl LocalClusterRunner {
    pub fn new(node_count: u32) -> Self {
        let (requests_tx, requests_rx) = mpsc::channel(1024);
        let (replies_tx, replies_rx) = mpsc::channel(1024);
        let mut c = Cluster::new(node_count, requests_rx, replies_tx);
        tokio::spawn(async move {
            c.run().await;
        });
        Self {
            node_count,
            tx: requests_tx,
            rx: replies_rx,
        }
    }

    pub async fn get_cluster_state(&mut self) -> Nodes {
        self.tx.send(CtlMsg::GetClusterState).await.unwrap();

        if let Some(CtlMsg::SendClusterState { nodes }) = self.rx.recv().await {
            Ok(nodes)
        } else {
            Err(eyre!("No state received!"))
        }
        .unwrap()
    }

    pub async fn sleep(&mut self, ms: u64) {
        sleep(Duration::from_millis(ms)).await;
    }

    pub async fn connect(&mut self, id: NodeId) {
        self.tx
            .send(CtlMsg::Connect { dest: id })
            .await
            .expect("Failed to send Connect");
    }

    pub async fn disconnect(&mut self, id: NodeId) {
        self.tx
            .send(CtlMsg::Disconnect { dest: id })
            .await
            .expect("Failed to send Disconnect");
    }

    // Only one leader at the highest term
    pub async fn check_one_leader(&mut self) -> Result<LocalNodeSummary, color_eyre::eyre::Error> {
        let iterations = 40;
        for _ in 0..iterations {
            self.sleep(100).await;
            let nodes = &self.get_cluster_state().await;
            let highest_leaders = leaders(nodes);
            match highest_leaders.len() {
                1 => return Ok(highest_leaders[0].clone()),
                l if l > 1 => bail!("More than one leader on the same term!"),
                _ => {}
            }
        }
        bail!("No leader elected in time!")
    }

    pub async fn send_cmd(&mut self, dest: NodeId, cmd: RaftCmd) {
        self.tx
            .send(CtlMsg::SendCmd { dest, cmd })
            .await
            .expect("Failed to send SendCmd");
    }

    pub async fn try_to_commit(&mut self, cmd: RaftCmd) -> Result<bool, color_eyre::eyre::Error> {
        let Ok(leader) = self.check_one_leader().await else {
            bail!("No single leader!");
        };
        let leader_id = leader.id;
        let term = leader.raft.term;
        let next_commit_id = leader.raft.commit_idx + 1;
        self.send_cmd(leader.id, cmd).await;
        for _ in 0..40 {
            self.sleep(100).await;
            let Ok(leader) = self.check_one_leader().await else {
                bail!("No single leader!");
            };
            if leader_id != leader.id {
                return Ok(false);
            }
            if leader.raft.commit_idx == next_commit_id
                && leader.raft.log[next_commit_id] == (term, cmd)
            {
                if self.is_highest_committed(next_commit_id).await {
                    return Ok(true);
                } else {
                    return Ok(false);
                }
            }
        }
        Ok(false)
    }

    pub async fn n_committed(&mut self, commit_idx: usize) -> u32 {
        let mut count = 0;
        for node in &self.get_cluster_state().await {
            if node.raft.commit_idx >= commit_idx {
                count += 1
            }
        }
        count
    }

    pub async fn is_highest_committed(&mut self, n: usize) -> bool {
        let mut count = 0;
        for node in &self.get_cluster_state().await {
            if node.raft.commit_idx == n {
                count += 1
            }
        }
        count > self.node_count / 2
    }

    pub async fn has_no_leader(&mut self) -> bool {
        leader_count(&self.get_cluster_state().await) == 0
    }

    pub async fn current_term(&mut self) -> u32 {
        let nodes = &self.get_cluster_state().await;
        let mut term = nodes[0].raft.term;
        for node in nodes.iter().skip(1) {
            let node_term = node.raft.term;
            if node_term > term {
                term = node_term
            }
        }
        term
    }

    pub async fn live_term_agreement(&mut self) -> bool {
        let nodes = &self.get_cluster_state().await;
        if nodes.len() <= 1 {
            return true;
        }
        let term = nodes[0].raft.term;
        for node in nodes.iter().skip(1) {
            if node.is_dead || node.is_disconnected {
                continue;
            }
            if node.raft.term != term {
                return false;
            }
        }
        true
    }
}

impl Drop for LocalClusterRunner {
    fn drop(&mut self) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            tx.send(CtlMsg::Shutdown)
                .await
                .expect("Failed to send Shutdown");
        });
    }
}

fn leader_count(nodes: &Nodes) -> u32 {
    leaders(nodes).len() as u32
}

fn leaders(nodes: &Nodes) -> Vec<LocalNodeSummary> {
    let mut highest_term = 0;
    let mut highest_leaders = Vec::new();
    for node in nodes {
        if node.is_dead || node.is_disconnected {
            continue;
        }
        if node.raft.term > highest_term {
            highest_term = node.raft.term;
            highest_leaders.clear();
        }
        if node.raft.term == highest_term && node.raft.is_leader {
            highest_leaders.push(node.clone());
        }
    }
    highest_leaders
}
