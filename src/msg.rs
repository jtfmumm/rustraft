pub type Term = u32;
pub type NodeId = usize;

#[derive(Clone, Debug)]
pub struct RaftNodeSummary {
    pub id: NodeId,
    pub term: Term,
    pub is_leader: bool,
}

#[derive(Clone, Debug)]
pub enum RaftMsg {
    RequestVote {
        term: Term,
        candidate: NodeId,
    },
    SendVote {
        dest: NodeId,
        term: Term,
        voter: NodeId,
    },
    AppendEntries {
        term: Term,
        leader: NodeId,
    },
    OutdatedTerm {
        dest: NodeId,
        outdated_term: Term,
        current_term: Term,
    },
}

#[derive(Clone, Debug)]
pub enum CtlMsg {
    GetClusterState,
    SendClusterState { nodes: Vec<RaftNodeSummary> },
    Kill { dest: NodeId },
    Start { dest: NodeId },
    Connect { dest: NodeId },
    Disconnect { dest: NodeId },
    Shutdown,
}
