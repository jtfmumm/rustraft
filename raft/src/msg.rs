use std::time::Duration;

pub type Term = u32;
pub type NodeId = usize;
pub type RaftCmd = u32;

pub const HEARTBEAT_DURATION: Duration = Duration::from_millis(150);
pub const ELECTION_TIMEOUT_MS_LOW: u64 = 500;
pub const ELECTION_TIMEOUT_MS_HIGH: u64 = 1000;

#[derive(Debug)]
pub enum RaftMsg {
    RequestVote {
        term: Term,
        candidate: NodeId,
        last_log_idx: usize,
        last_log_term: Term,
    },
    SendVote {
        term: Term,
        voter: NodeId,
    },
    AppendEntries {
        term: Term,
        leader: NodeId,
        prev_log_idx: usize,
        prev_log_term: Term,
        entries: Vec<(Term, RaftCmd)>,
        leader_commit_idx: usize,
    },
    AppendEntriesReply {
        term: Term,
        commit_idx: usize,
        replier: NodeId,
        is_success: bool,
    },
    OutdatedTerm {
        outdated_term: Term,
        current_term: Term,
    },
}

///////////////////////////////////////////////////
// The rest of this module supports local testing.
///////////////////////////////////////////////////
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftNodeSummary {
    pub id: NodeId,
    pub term: Term,
    pub is_leader: bool,
    pub commit_idx: usize,
    pub log: Vec<(Term, RaftCmd)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalNodeSummary {
    pub id: NodeId,
    pub is_dead: bool,
    pub is_disconnected: bool,
    pub raft: RaftNodeSummary,
}

#[derive(Clone, Debug)]
pub enum CtlMsg {
    SendCmd { dest: NodeId, cmd: RaftCmd },
    GetClusterState,
    SendClusterState { nodes: Vec<LocalNodeSummary> },
    Kill { dest: NodeId },
    Start { dest: NodeId },
    Connect { dest: NodeId },
    Disconnect { dest: NodeId },
    Shutdown,
}

impl CtlMsg {
    pub fn destinations(&self) -> Destinations {
        use CtlMsg::*;
        use Destinations::*;
        match self {
            SendCmd { dest, .. } => One(*dest),
            GetClusterState => All,
            SendClusterState { .. } => None,
            Kill { dest } => One(*dest),
            Start { dest } => One(*dest),
            Connect { dest } => One(*dest),
            Disconnect { dest } => One(*dest),
            Shutdown => All,
        }
    }
}

pub enum Destinations {
    All,
    None,
    One(NodeId),
}
