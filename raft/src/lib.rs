use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use rand::Rng;
use std::{
    cmp,
    time::Duration,
    iter::zip,
};
use tokio::{sync::{mpsc, oneshot}, time::{self, Instant}};

pub mod msg;

use msg::{
    NodeId, RaftCmd, RaftMsg, RaftNodeSummary, Term, ELECTION_TIMEOUT_MS_HIGH,
    ELECTION_TIMEOUT_MS_LOW, HEARTBEAT_DURATION,
};

#[derive(Clone, Debug)]
pub enum RaftState {
    Candidate {
        votes: HashSet<NodeId>,
    },
    Follower,
    Leader {
        // For each node, idx of the next log entry to send to that node
        // initialized to leader last log index + 1
        next_idxs: HashMap<NodeId, usize>,
        // For each node, idx of highest log entry known to be replicated to that node
        // initialized to 0, increases monotonically
        match_idxs: HashMap<NodeId, usize>,
    },
}

// Implementation of Raft protocol.
pub struct RaftNode {
    pub id: NodeId,
    state: RaftState,
    /// We currently assume membership goes from 0 to node_count - 1
    node_count: u32,
    term: Term,
    log: Vec<(Term, RaftCmd)>,
    /// Next timeout
    next_timeout: Instant,
    // index of highest log entry known to be committed
    commit_idx: usize,
    // Channels
    /// RaftMsgs sent between Raft nodes
    incoming_network_rx: mpsc::Receiver<RaftMsg>,
    outgoing_network_tx: mpsc::Sender<(NodeId, RaftMsg)>,
    /// Cmds sent from client/s
    client_rx: mpsc::Receiver<RaftCmd>,
    /// Channel for testing
    summary_request_rx: mpsc::Receiver<oneshot::Sender<RaftNodeSummary>>,
}

impl RaftNode {
    pub fn new(
        node_count: u32,
        id: NodeId,
        incoming_network_rx: mpsc::Receiver<RaftMsg>,
        outgoing_network_tx: mpsc::Sender<(NodeId, RaftMsg)>,
        client_rx: mpsc::Receiver<RaftCmd>,
        summary_request_rx: mpsc::Receiver<oneshot::Sender<RaftNodeSummary>>,
    ) -> Self {
        let mut node = Self {
            id,
            state: RaftState::Follower,
            node_count,
            term: 0,
            // The 0th entry is just a placeholder
            log: vec![(0, 0)],
            next_timeout: Instant::now() + random_timeout(),
            commit_idx: 0,
            incoming_network_rx,
            outgoing_network_tx,
            client_rx,
            summary_request_rx,
        };
        node.init();
        node
    }

    pub fn init(&mut self) {
        let state = RaftState::Follower;
        self.state = state;
        self.term = 0;
        // 0th index is a placeholder
        self.log = vec![(0, 0)];
        // TODO: Create log from persisted state, if any
        self.reset_timeout();
    }

    pub async fn run(&mut self) {
        loop {
            let sleep = time::sleep_until(self.next_timeout);
            tokio::select! {
                Some(msg) = self.incoming_network_rx.recv() => {
                    self.receive_message(msg).await;
                }
                Some(cmd) = self.client_rx.recv() => {
                    self.apply_cmd(cmd).await;
                }
                Some(oneshot_ch) = self.summary_request_rx.recv() => {
                    oneshot_ch.send(self.get_state()).unwrap();
                }
                _ = sleep => {
                    self.handle_timeout().await;
                }
            }
        }
    }

    async fn apply_cmd(&mut self, cmd: RaftCmd) {
        use RaftState::*;
        match &self.state {
            Leader {
                next_idxs: _,
                match_idxs: _,
            } => {
                self.log.push((self.term, cmd));

                let prev_log_idx = self.commit_idx;
                let prev_log_term = self.log[self.commit_idx].0;
                let entries = vec![(self.term, cmd)];
                for node_id in 0..self.node_count as usize {
                    if node_id == self.id {
                        continue;
                    }
                    let msg = self.create_append_entries(
                        prev_log_idx,
                        prev_log_term,
                        entries.clone(),
                    );
                    self.outgoing_network_tx.send((node_id, msg)).await.expect("Failed to send RaftMsg!");
                }
            }
            _ => {
                println!("Received apply_cmd at non-leader! id {}", self.id);
            }
        }
    }

    async fn receive_message(&mut self, msg: RaftMsg) {
        use RaftMsg::*;
        match msg {
            RequestVote {
                term,
                candidate,
                last_log_idx,
                last_log_term,
            } => self.receive_vote_request(term, candidate, last_log_idx, last_log_term).await,
            SendVote {
                term,
                voter,
            } => self.receive_vote(term, voter).await,
            AppendEntries {
                term,
                leader,
                prev_log_term,
                prev_log_idx,
                entries,
                leader_commit_idx,
            } => self.receive_append_entries(
                term,
                leader,
                prev_log_idx,
                prev_log_term,
                entries,
                leader_commit_idx,
            ).await,
            AppendEntriesReply {
                term,
                commit_idx,
                replier,
                is_success,
            } => self.receive_append_entries_reply(term, commit_idx, replier, is_success).await,
            OutdatedTerm {
                outdated_term,
                current_term,
            } => self.receive_outdated_term(outdated_term, current_term).await,
        };
    }

    async fn handle_timeout(&mut self) {
        use RaftState::*;
        match &self.state {
            Candidate { .. } => {
                self.transition_to_follower();
            }
            Follower => {
                self.term += 1;
                let last_log_idx = self.log.len() - 1;
                let last_log_term = self.log[last_log_idx].0;
                for node_id in 0..self.node_count as usize {
                    if node_id == self.id {
                        continue;
                    }
                    let msg = RaftMsg::RequestVote {
                        term: self.term,
                        candidate: self.id,
                        last_log_idx,
                        last_log_term,
                    };
                    self.outgoing_network_tx.send((node_id, msg)).await.expect("Failed to send RaftMsg!");
                }
                self.transition_to_candidate();
            }
            Leader { .. } => {
                let prev_log_idx = self.commit_idx;
                let prev_log_term = self.log[self.commit_idx].0;
                let empty_entries = Vec::new();
                for node_id in 0..self.node_count as usize {
                    if node_id == self.id {
                        continue;
                    }
                    let msg = self.create_append_entries(
                        prev_log_idx,
                        prev_log_term,
                        empty_entries.clone(),
                    );
                    self.outgoing_network_tx.send((node_id, msg)).await.expect("Failed to send RaftMsg!");
                }
                self.next_timeout = Instant::now() + HEARTBEAT_DURATION;
            }
        }
    }

    pub fn get_state(&self) -> RaftNodeSummary {
        RaftNodeSummary {
            id: self.id,
            term: self.term,
            is_leader: matches!(self.state, RaftState::Leader { .. }),
            commit_idx: self.commit_idx,
            log: self.log.clone(),
        }
    }

    async fn receive_vote_request(
        &mut self,
        term: Term,
        candidate: NodeId,
        last_log_idx: usize,
        last_log_term: Term,
    ) {
        println!(
            "{} receive_vote_request: {term} {candidate}. My term: {}",
            self.id, self.term
        );
        if term <= self.term {
            return;
        }

        self.term = term;

        let local_last_log_idx = self.log.len() - 1;
        let local_last_log_term = self.log[local_last_log_idx].0;
        let veto = last_log_term < local_last_log_term
            || (last_log_term == local_last_log_term && last_log_idx < local_last_log_idx);
        if veto {
            return;
        }

        let vote = RaftMsg::SendVote {
            term,
            voter: self.id,
        };
        use RaftState::*;
        if matches!(self.state, Candidate { .. } | Leader { .. }) {
            self.transition_to_follower();
        }
        self.outgoing_network_tx.send((candidate, vote)).await.expect("Failed to send Vote!");
    }

    async fn receive_vote(&mut self, term: Term, voter: NodeId) {
        use RaftState::*;
        if let Candidate { ref mut votes } = &mut self.state {
            println!(
                "{} receive_vote: {term} {voter}. Total: {}",
                self.id,
                votes.len()
            );
            if term == self.term {
                votes.insert(voter);
                if votes.len() > (self.node_count / 2) as usize {
                    println!("{}: I'm leader with {} votes!", self.id, votes.len());
                    self.transition_to_leader();
                    // Any logs later than commit idx are no longer valid
                    self.log.truncate(self.commit_idx + 1);
                    let prev_log_idx = self.commit_idx;
                    let prev_log_term = self.log[self.commit_idx].0;
                    let entries = Vec::new();
                    for node_id in 0..self.node_count as usize {
                        if node_id == self.id {
                            continue;
                        }
                        let msg = self.create_append_entries(
                            prev_log_idx,
                            prev_log_term,
                            entries.clone(),
                        );
                        self.outgoing_network_tx.send((node_id, msg)).await.expect("Failed to send RaftMsg!");
                    }
                }
            }
        }
    }

    async fn receive_append_entries(
        &mut self,
        term: Term,
        leader: NodeId,
        prev_log_idx: usize,
        prev_log_term: Term,
        entries: Vec<(Term, RaftCmd)>,
        leader_commit_idx: usize,
    ) {
        println!("{} receive_append_entries: {term} {leader}", self.id);
        use RaftState::*;
        // First check if we need to transition to a follower.
        if term >= self.term && matches!(self.state, Candidate { .. } | Leader { .. }) {
            self.term = term;
            self.transition_to_follower();
        }
        // Then check if we're a follower
        if matches!(self.state, Follower) {
            if term >= self.term {
                self.term = term;
                if leader_commit_idx > self.commit_idx {
                    // TODO: Should we be storing the leader commit idx or the more
                    // optimistic prev_log_idx + entries.len()? I think the latter
                    // is safe since followers never serve replies (and must agree
                    // with a majority to be elected).
                    self.commit_idx = leader_commit_idx
                }
                self.reset_timeout();

                // Are we ready for this new list of entries?
                if prev_log_idx >= self.log.len() || self.log[prev_log_idx].0 != prev_log_term {
                    let msg = RaftMsg::AppendEntriesReply {
                        commit_idx: self.commit_idx,
                        term,
                        replier: self.id,
                        is_success: false,
                    };
                    self.outgoing_network_tx.send((leader, msg)).await.expect("Failed to send RaftMsg!");
                    return;
                }

                if entries.is_empty() {
                    return;
                }
                let start_idx = prev_log_idx + 1;
                for (entry, idx) in zip(entries.clone(), start_idx..start_idx + entries.len()) {
                    if idx == self.log.len() {
                        self.log.push(entry);
                    } else {
                        self.log[idx] = entry;
                    }
                }
                // The latest index at which the leader is trying to commit
                let commit_idx = prev_log_idx + entries.len();
                let msg = RaftMsg::AppendEntriesReply {
                    commit_idx,
                    term,
                    replier: self.id,
                    is_success: true,
                };
                self.outgoing_network_tx.send((leader, msg)).await.expect("Failed to send RaftMsg!");
                /////////////////////////
            } else {
                let msg = RaftMsg::OutdatedTerm {
                    outdated_term: term,
                    current_term: self.term,
                };
                self.outgoing_network_tx.send((leader, msg)).await.expect("Failed to send RaftMsg!");
            }
        }
    }

    async fn receive_append_entries_reply(
        &mut self,
        term: Term,
        commit_idx: usize,
        replier: NodeId,
        is_success: bool,
    ) {
        println!(
            "{} receive_append_entries: term {term} from {replier}, is_success: {is_success}",
            self.id
        );
        if let RaftState::Leader {
            next_idxs,
            ref mut match_idxs,
        } = &mut self.state {
            if is_success {
                let high_idx = cmp::max(commit_idx, *match_idxs.get(&replier).unwrap());
                match_idxs.insert(replier, high_idx);
                if commit_idx > self.commit_idx {
                    let mut count = 0;
                    for idx in match_idxs.values() {
                        if *idx >= commit_idx {
                            count += 1;
                        }
                    }
                    // The leader implicitly counts itself
                    if count >= self.node_count / 2 {
                        self.commit_idx = commit_idx;
                    }
                }
            } else {
                let old_next_idx = next_idxs.get(&replier).unwrap();
                // TODO: Is this the right way to handle this?
                // This indicates we are an outdated leader.
                if *old_next_idx <= 1 {
                    return;
                }
                let new_next_idx = old_next_idx - 1;
                next_idxs.insert(replier, new_next_idx);
                let prev_log_idx = new_next_idx - 1;
                let prev_log_term = self.log[prev_log_idx].0;
                let entries =
                    Vec::from_iter(self.log[new_next_idx..self.log.len()].iter().cloned());
                let msg = self.create_append_entries(
                    prev_log_idx,
                    prev_log_term,
                    entries.clone(),
                );
                self.outgoing_network_tx.send((replier, msg)).await.expect("Failed to send RaftMsg!");
            }
        }
    }

    async fn receive_outdated_term(&mut self, outdated_term: Term, current_term: Term) {
        println!("{} receive_outdated_term: Outdated: {outdated_term}, 'Current': {current_term}. My term: {}", self.id, self.term);
        if outdated_term < self.term {
            return;
        }

        self.term = current_term;
        use RaftState::*;
        if matches!(self.state, Candidate { .. } | Leader { .. }) {
            self.transition_to_follower();
        }
    }

    fn reset_timeout(&mut self) {
        self.next_timeout = Instant::now() + random_timeout();
    }

    // TODO: Provide implementation of persistent store to struct
    // fn write_log(&self) {
    // }

    // TODO: Provide implementation of persistent store to struct
    // fn restore_from_log(&mut self) {
    // }

    fn transition_to_candidate(&mut self) {
        let mut votes = HashSet::new();
        votes.insert(self.id);
        self.reset_timeout();
        self.state = RaftState::Candidate { votes };
    }

    fn transition_to_follower(&mut self) {
        self.reset_timeout();
        self.state = RaftState::Follower;
    }

    fn transition_to_leader(&mut self) {
        self.next_timeout = Instant::now() + HEARTBEAT_DURATION;
        let mut next_idxs = HashMap::new();
        let mut match_idxs = HashMap::new();
        for id in 0..self.node_count {
            next_idxs.insert(id as usize, self.log.len());
            match_idxs.insert(id as usize, 0);
        }
        self.state = RaftState::Leader {
            next_idxs,
            match_idxs,
        };
    }

    fn create_append_entries(
        &self,
        prev_log_idx: usize,
        prev_log_term: Term,
        entries: Vec<(Term, RaftCmd)>,
    ) -> RaftMsg {
        RaftMsg::AppendEntries {
            term: self.term,
            leader: self.id,
            prev_log_idx,
            prev_log_term,
            entries,
            leader_commit_idx: self.commit_idx,
        }
    }
}

fn random_timeout() -> Duration {
    let mut rng = rand::thread_rng();
    let ms: u64 = rng.gen_range(ELECTION_TIMEOUT_MS_LOW..ELECTION_TIMEOUT_MS_HIGH);
    Duration::from_millis(ms)
}
