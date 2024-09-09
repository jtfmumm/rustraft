use std::time::Duration;

use color_eyre::eyre::{bail, eyre};
use rand::prelude::SliceRandom;
use tokio::sync::mpsc;
use tokio::time::sleep;

use rustraft::cluster::Cluster;
use rustraft::msg::{CtlMsg, NodeId, RaftNodeSummary};

type Nodes = Vec<RaftNodeSummary>;

struct TestRun {
    tx: mpsc::Sender<CtlMsg>,
    rx: mpsc::Receiver<CtlMsg>,
}

impl TestRun {
    pub fn new(node_count: u32) -> Self {
        let (requests_tx, requests_rx) = mpsc::channel(32);
        let (replies_tx, replies_rx) = mpsc::channel(32);
        let mut c = Cluster::new(node_count, requests_rx, replies_tx);
        tokio::spawn(async move {
            c.run().await;
        });
        Self {
            tx: requests_tx,
            rx: replies_rx,
        }
    }

    pub async fn get_state(&mut self) -> Nodes {
        self.tx.send(CtlMsg::GetClusterState).await;

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
        self.tx.send(CtlMsg::Connect { dest: id }).await;
    }

    pub async fn disconnect(&mut self, id: NodeId) {
        self.tx.send(CtlMsg::Disconnect { dest: id }).await;
    }

    // Only one leader at the highest term
    pub async fn check_one_leader(&mut self) -> Result<NodeId, color_eyre::eyre::Error> {
        let iterations = 40;
        for _ in 0..iterations {
            self.sleep(100).await;
            let nodes = &self.get_state().await;
            let highest_leaders = leaders(nodes);
            match highest_leaders.len() {
                1 => return Ok(highest_leaders[0]),
                l if l > 1 => bail!("More than one leader on the same term!"),
                _ => {}
            }
        }
        bail!("No leader elected in time!")
    }

    pub async fn has_no_leader(&mut self) -> bool {
        leader_count(&self.get_state().await) == 0
    }

    pub async fn current_term(&mut self) -> u32 {
        let nodes = &self.get_state().await;
        let mut term = nodes[0].term;
        for node in nodes.iter().skip(1) {
            let node_term = node.term;
            if node_term > term {
                term = node_term
            }
        }
        term
    }

    pub async fn term_agreement(&mut self) -> bool {
        let nodes = &self.get_state().await;
        if nodes.len() <= 1 {
            return true;
        }
        let term = nodes[0].term;
        for node in nodes.iter().skip(1) {
            if node.term != term {
                return false;
            }
        }
        true
    }
}

impl Drop for TestRun {
    fn drop(&mut self) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            tx.send(CtlMsg::Shutdown).await;
        });
    }
}

#[tokio::test]
async fn initial_election() {
    let mut tr = TestRun::new(10);
    assert!(tr.has_no_leader().await);

    tr.check_one_leader().await.unwrap();
    let term1 = tr.current_term().await;
    tr.sleep(500).await;
    assert!(tr.term_agreement().await);
    tr.sleep(1500).await;
    assert_eq!(term1, tr.current_term().await);
    tr.check_one_leader().await.unwrap();
}

#[tokio::test]
async fn re_election() {
    let node_count: usize = 3;
    let mut tr = TestRun::new(node_count as u32);

    let leader1 = tr.check_one_leader().await.unwrap();

    tr.disconnect(leader1).await;
    let leader2 = tr.check_one_leader().await.unwrap();

    tr.connect(leader1).await;
    assert_eq!(leader2, tr.check_one_leader().await.unwrap());

    tr.disconnect(leader2).await;
    tr.disconnect((leader2 + 1) % node_count).await;
    tr.sleep(4000).await;
    assert!(tr.has_no_leader().await);

    tr.connect((leader2 + 1) % node_count).await;
    let leader3 = tr.check_one_leader().await.unwrap();

    tr.connect(leader2).await;
    tr.sleep(2000).await;
    assert_eq!(leader3, tr.check_one_leader().await.unwrap());
}

#[tokio::test]
async fn multiple_elections() {
    let node_count: usize = 7;
    let mut tr = TestRun::new(node_count as u32);

    tr.check_one_leader().await.unwrap();
    for _ in 0..10 {
        // disconnect three nodes
        let mut choices: Vec<usize> = (0..node_count).collect();
        choices.shuffle(&mut rand::thread_rng());
        let n1 = choices[0];
        let n2 = choices[1];
        let n3 = choices[2];
        tr.disconnect(n1).await;
        tr.disconnect(n2).await;
        tr.disconnect(n3).await;

        // either the current leader should still be alive,
        // or the remaining four should elect a new one.
        tr.check_one_leader().await.unwrap();

        tr.connect(n1).await;
        tr.connect(n2).await;
        tr.connect(n3).await;
    }
    tr.check_one_leader().await.unwrap();
}

fn leader_count(nodes: &Nodes) -> u32 {
    leaders(nodes).len() as u32
}

fn leaders(nodes: &Nodes) -> Vec<NodeId> {
    let mut highest_term = 0;
    let mut highest_leaders = Vec::new();
    for node in nodes {
        if node.is_leader {
            match node.term {
                t if t == highest_term => highest_leaders.push(node.id),
                t if t > highest_term => {
                    highest_term = node.term;
                    highest_leaders.clear();
                    highest_leaders.push(node.id);
                }
                _ => {}
            }
        } else if node.term > highest_term {
            highest_term = node.term;
            highest_leaders.clear();
        }
    }
    highest_leaders
}
