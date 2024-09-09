use ahash::{HashSet, HashSetExt};
use rand::Rng;
use std::time::{Duration, SystemTime};

use crate::msg::{NodeId, RaftMsg, RaftNodeSummary, Term};

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);
const ELECTION_TIMEOUT_MS_LOW: u64 = 500;
const ELECTION_TIMEOUT_MS_HIGH: u64 = 1000;

#[derive(Clone, Debug)]
pub enum RaftState {
    Candidate { votes: HashSet<NodeId> },
    Follower,
    Leader,
}

impl RaftState {
    pub fn init() -> Self {
        RaftState::Follower
    }
}

pub struct RaftNode {
    pub id: NodeId,
    pub state: RaftState,
    node_count: u32,
    pub term: Term,
    log: Vec<(Term, String)>,
    cur_timeout: Duration,
    last_signal: SystemTime,
}

impl RaftNode {
    pub fn new(node_count: u32, id: NodeId) -> Self {
        let mut node = Self {
            id,
            state: RaftState::init(),
            node_count,
            term: 0,
            log: Vec::new(),
            cur_timeout: random_timeout(),
            last_signal: SystemTime::now(),
        };
        node.init();
        node
    }

    pub fn init(&mut self) {
        let state = RaftState::init();
        self.state = state;
        self.term = 0;
        self.log = Vec::new();
        self.reset_timeout();

        // self.log = self.restore_from_log();
    }

    pub fn reset_timeout(&mut self) {
        self.cur_timeout = random_timeout();
        self.last_signal = SystemTime::now();
    }

    // fn write_log(&self) {
    // }

    // fn restore_from_log(&mut self) {
    // }

    pub fn receive_message(&mut self, msg: RaftMsg) -> Option<RaftMsg> {
        use RaftMsg::*;
        match msg {
            RequestVote { term, candidate } => self.receive_vote_request(term, candidate),
            SendVote {
                dest: _,
                term,
                voter,
            } => self.receive_vote(term, voter),
            AppendEntries { term, leader } => self.receive_append_entries(term, leader),
            OutdatedTerm {
                dest: _,
                outdated_term,
                current_term,
            } => self.receive_outdated_term(outdated_term, current_term),
        }
    }

    pub fn tick(&mut self) -> Option<RaftMsg> {
        use RaftState::*;
        match self.state {
            Candidate { .. } => {
                if self.last_signal.elapsed().unwrap() > self.cur_timeout {
                    self.transition_to_follower();
                }
            }
            Follower => {
                if self.last_signal.elapsed().unwrap() > self.cur_timeout {
                    self.term += 1;
                    let msg = RaftMsg::RequestVote {
                        term: self.term,
                        candidate: self.id,
                    };
                    self.transition_to_candidate();
                    return Some(msg);
                }
            }
            Leader => {
                if self.last_signal.elapsed().unwrap() > self.cur_timeout {
                    let msg = RaftMsg::AppendEntries {
                        term: self.term,
                        leader: self.id,
                    };
                    self.reset_timeout();
                    self.cur_timeout = HEARTBEAT_INTERVAL;
                    return Some(msg);
                }
            }
        }
        None
    }

    fn receive_vote_request(&mut self, term: Term, candidate: NodeId) -> Option<RaftMsg> {
        println!(
            "{} receive_vote_request: {term} {candidate}. My term: {}",
            self.id, self.term
        );
        if term <= self.term {
            return None;
        }

        self.term = term;
        use RaftState::*;
        match self.state {
            Candidate { .. } => {
                let vote = RaftMsg::SendVote {
                    dest: candidate,
                    term,
                    voter: self.id,
                };
                self.transition_to_follower();
                Some(vote)
            }
            Follower => {
                let vote = RaftMsg::SendVote {
                    dest: candidate,
                    term,
                    voter: self.id,
                };
                Some(vote)
            }
            Leader { .. } => {
                let vote = RaftMsg::SendVote {
                    dest: candidate,
                    term,
                    voter: self.id,
                };
                self.transition_to_follower();
                Some(vote)
            }
        }
    }

    fn receive_vote(&mut self, term: Term, voter: NodeId) -> Option<RaftMsg> {
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
                    let msg = RaftMsg::AppendEntries {
                        term,
                        leader: self.id,
                    };
                    return Some(msg);
                }
            }
        }
        None
    }

    fn receive_append_entries(&mut self, term: Term, leader: NodeId) -> Option<RaftMsg> {
        println!("{} receive_append_entries: {term} {leader}", self.id);
        use RaftState::*;
        match self.state {
            Candidate { .. } | Leader { .. } => {
                if term >= self.term {
                    self.term = term;
                    self.transition_to_follower();
                }
            }
            Follower => {
                if term >= self.term {
                    self.term = term;
                    self.reset_timeout();
                } else {
                    let msg = RaftMsg::OutdatedTerm {
                        dest: leader,
                        outdated_term: term,
                        current_term: self.term,
                    };
                    return Some(msg);
                }
            }
        }
        None
    }

    pub fn receive_outdated_term(
        &mut self,
        outdated_term: Term,
        current_term: Term,
    ) -> Option<RaftMsg> {
        println!("{} receive_outdated_term: Outdated: {outdated_term}, 'Current': {current_term}. My term: {}", self.id, self.term);
        if outdated_term < self.term {
            return None;
        }

        self.term = current_term;
        use RaftState::*;
        match self.state {
            Candidate { .. } | Leader { .. } => {
                // let state = self.state;
                self.transition_to_follower();
            }
            Follower => {}
        }
        None
    }

    pub fn get_state(&self) -> RaftNodeSummary {
        RaftNodeSummary {
            id: self.id,
            term: self.term,
            is_leader: matches!(self.state, RaftState::Leader),
        }
    }

    pub fn transition_to_candidate(&mut self) {
        let mut votes = HashSet::new();
        votes.insert(self.id);
        self.reset_timeout();
        self.state = RaftState::Candidate { votes };
    }

    pub fn transition_to_follower(&mut self) {
        self.reset_timeout();
        self.state = RaftState::Follower;
    }

    pub fn transition_to_leader(&mut self) {
        self.reset_timeout();
        self.cur_timeout = HEARTBEAT_INTERVAL;
        self.state = RaftState::Leader;
    }
}

fn random_timeout() -> Duration {
    let mut rng = rand::thread_rng();
    let ms: u64 = rng.gen_range(ELECTION_TIMEOUT_MS_LOW..ELECTION_TIMEOUT_MS_HIGH);
    Duration::from_millis(ms)
}
