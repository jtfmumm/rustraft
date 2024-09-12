use rand::prelude::SliceRandom;

use rustraft_local_cluster::local_cluster_tester::LocalClusterRunner;
use rustraft_raft::msg::{NodeId, RaftCmd, ELECTION_TIMEOUT_MS_HIGH};

/// Raft protocol tests ported from Go tests used for an MIT Raft lab
/// https://pdos.csail.mit.edu/6.824/labs/lab-raft.html

#[tokio::test]
async fn initial_election() {
    let mut cr = LocalClusterRunner::new(10);
    assert!(cr.has_no_leader().await);

    cr.check_one_leader().await.unwrap();
    let term1 = cr.current_term().await;
    cr.sleep(500).await;
    assert!(cr.live_term_agreement().await);
    cr.sleep(1500).await;
    assert_eq!(term1, cr.current_term().await);
    cr.check_one_leader().await.unwrap();
}

/// election after network failure
#[tokio::test]
async fn re_election() {
    let node_count: usize = 3;
    let mut cr = LocalClusterRunner::new(node_count as u32);

    let leader1 = cr.check_one_leader().await.unwrap();

    cr.disconnect(leader1.id).await;
    let leader2 = cr.check_one_leader().await.unwrap();

    cr.connect(leader1.id).await;
    assert_eq!(leader2, cr.check_one_leader().await.unwrap());

    cr.disconnect(leader2.id).await;
    cr.disconnect((leader2.id + 1) % node_count).await;
    cr.sleep(4 * ELECTION_TIMEOUT_MS_HIGH).await;
    assert!(cr.has_no_leader().await);

    cr.connect((leader2.id + 1) % node_count).await;
    let leader3 = cr.check_one_leader().await.unwrap();

    cr.connect(leader2.id).await;
    cr.sleep(2000).await;
    assert_eq!(leader3, cr.check_one_leader().await.unwrap());
}

#[tokio::test]
async fn multiple_elections() {
    let node_count: usize = 7;
    let mut cr = LocalClusterRunner::new(node_count as u32);

    cr.check_one_leader().await.unwrap();
    for _ in 0..10 {
        // disconnect three nodes
        let mut choices: Vec<usize> = (0..node_count).collect();
        choices.shuffle(&mut rand::thread_rng());
        let n1 = choices[0];
        let n2 = choices[1];
        let n3 = choices[2];
        cr.disconnect(n1).await;
        cr.disconnect(n2).await;
        cr.disconnect(n3).await;

        // either the current leader should still be alive,
        // or the remaining four should elect a new one.
        cr.check_one_leader().await.unwrap();

        cr.connect(n1).await;
        cr.connect(n2).await;
        cr.connect(n3).await;
    }
    cr.check_one_leader().await.unwrap();
}

#[tokio::test]
async fn basic_agreement() {
    let node_count: usize = 3;
    let mut cr = LocalClusterRunner::new(node_count as u32);

    cr.check_one_leader().await.unwrap();
    for i in 1..4 {
        assert_eq!(cr.n_committed(i).await, 0);
        assert!(cr.try_to_commit(i as RaftCmd).await.unwrap());
    }
}

/// Test just failure of followers.
#[tokio::test]
async fn follower_failures() {
    let node_count: usize = 3;
    let mut cr = LocalClusterRunner::new(node_count as u32);

    cr.try_to_commit(101 as RaftCmd).await.unwrap();

    let leader1 = cr.check_one_leader().await.unwrap();
    cr.disconnect((leader1.id + 1) % node_count).await;

    // the leader and remaining follower should be
    // able to agree despite the disconnected follower.
    cr.try_to_commit(102 as RaftCmd).await.unwrap();
    cr.sleep(ELECTION_TIMEOUT_MS_HIGH).await;
    cr.try_to_commit(103 as RaftCmd).await.unwrap();

    // disconnect the remaining follower
    let leader2 = cr.check_one_leader().await.unwrap();
    cr.disconnect((leader2.id + 1) % node_count).await;
    cr.disconnect((leader2.id + 2) % node_count).await;

    // submit a command.
    cr.try_to_commit(104 as RaftCmd).await.unwrap();
    cr.sleep(2 * ELECTION_TIMEOUT_MS_HIGH).await;

    // check that command 104 did not commit.
    assert!(cr.n_committed(3).await > 0);
    assert_eq!(cr.n_committed(4).await, 0);
}

/// Test just failure of leaders.
#[tokio::test]
async fn leader_failures() {
    let node_count: usize = 3;
    let mut cr = LocalClusterRunner::new(node_count as u32);

    cr.try_to_commit(101 as RaftCmd).await.unwrap();

    let leader1 = cr.check_one_leader().await.unwrap();
    cr.disconnect(leader1.id).await;

    // the remaining followers should elect
    // a new leader.
    cr.try_to_commit(102 as RaftCmd).await.unwrap();
    cr.sleep(ELECTION_TIMEOUT_MS_HIGH).await;
    cr.try_to_commit(103 as RaftCmd).await.unwrap();

    // disconnect the new leader
    let leader2 = cr.check_one_leader().await.unwrap();
    cr.disconnect(leader2.id).await;

    // submit a command to each server.
    for i in 0..node_count {
        cr.send_cmd(i as NodeId, 104).await;
    }
    cr.sleep(2 * ELECTION_TIMEOUT_MS_HIGH).await;

    // check that command 104 did not commit.
    assert!(cr.n_committed(3).await > node_count as u32 / 2);
    assert_eq!(cr.n_committed(4).await, 0);
}

/// agreement after follower reconnects
#[tokio::test]
async fn fail_agree() {
    let node_count: usize = 3;
    let mut cr = LocalClusterRunner::new(node_count as u32);

    cr.try_to_commit(101 as RaftCmd).await.unwrap();

    // Disconnect a follower
    let leader1 = cr.check_one_leader().await.unwrap();
    cr.disconnect((leader1.id + 1) % node_count as NodeId).await;

    // the leader and remaining follower should be
    // able to agree despite the disconnected follower.
    cr.try_to_commit(102 as RaftCmd).await.unwrap();
    cr.try_to_commit(103 as RaftCmd).await.unwrap();
    cr.sleep(ELECTION_TIMEOUT_MS_HIGH).await;
    cr.try_to_commit(104 as RaftCmd).await.unwrap();
    cr.try_to_commit(105 as RaftCmd).await.unwrap();

    // re-connect
    cr.connect((leader1.id + 1) % node_count as NodeId).await;

    // the full set of servers should preserve
    // previous agreements, and be able to agree
    // on new commands.
    cr.try_to_commit(106 as RaftCmd).await.unwrap();
    cr.sleep(ELECTION_TIMEOUT_MS_HIGH).await;
    cr.try_to_commit(107 as RaftCmd).await.unwrap();

    assert!(cr.n_committed(7).await > node_count as u32 / 2);
}

/// no agreement if too many followers disconnect
#[tokio::test]
async fn fail_no_agree() {
    let node_count: usize = 5;
    let mut cr = LocalClusterRunner::new(node_count as u32);

    cr.try_to_commit(10 as RaftCmd).await.unwrap();

    // Disconnect 3 of 5 followers
    let leader1 = cr.check_one_leader().await.unwrap();
    cr.disconnect((leader1.id + 1) % node_count as NodeId).await;
    cr.disconnect((leader1.id + 2) % node_count as NodeId).await;
    cr.disconnect((leader1.id + 3) % node_count as NodeId).await;

    // the leader should not commit
    assert!(!cr.try_to_commit(30 as RaftCmd).await.unwrap());

    cr.sleep(2 * ELECTION_TIMEOUT_MS_HIGH).await;

    cr.disconnect(leader1.id).await;
    // repair
    cr.connect((leader1.id + 1) % node_count as NodeId).await;
    cr.connect((leader1.id + 2) % node_count as NodeId).await;
    cr.connect((leader1.id + 3) % node_count as NodeId).await;

    // let the disconnected majority choose a leader from
    // among their own ranks.
    cr.check_one_leader().await.unwrap();
    cr.try_to_commit(30 as RaftCmd).await.unwrap();

    // Bring old leader back
    cr.connect(leader1.id).await;

    cr.try_to_commit(1000 as RaftCmd).await.unwrap();

    assert!(cr.n_committed(3).await > node_count as u32 / 2);
    assert!(cr.n_committed(4).await <= node_count as u32 / 2);
}

/// rejoin of partitioned leader
#[tokio::test]
async fn rejoin() {
    let node_count: usize = 3;
    let mut cr = LocalClusterRunner::new(node_count as u32);

    cr.try_to_commit(101 as RaftCmd).await.unwrap();

    // Leader network failure
    let leader1 = cr.check_one_leader().await.unwrap();
    cr.disconnect(leader1.id).await;

    // make old leader try to agree on some entries
    cr.send_cmd(leader1.id, 102).await;
    cr.send_cmd(leader1.id, 103).await;
    cr.send_cmd(leader1.id, 104).await;

    // new leader commits, also for index=2
    cr.try_to_commit(103 as RaftCmd).await.unwrap();

    // new leader network failure
    let leader2 = cr.check_one_leader().await.unwrap();
    cr.disconnect(leader2.id).await;

    // old leader connected again
    cr.connect(leader1.id).await;
    cr.try_to_commit(104 as RaftCmd).await.unwrap();

    // all together now
    cr.connect(leader2.id).await;

    cr.try_to_commit(105 as RaftCmd).await.unwrap();

    assert!(cr.n_committed(4).await > node_count as u32 / 2);
    assert!(cr.n_committed(5).await == 0);
}

/// leader backs up quickly over incorrect follower logs
#[tokio::test]
async fn backup() {
    let node_count: usize = 5;
    let mut cr = LocalClusterRunner::new(node_count as u32);

    let mut next_cmd: RaftCmd = 100;

    next_cmd += 1;
    cr.try_to_commit(next_cmd).await.unwrap();

    // put leader and one follower in a partition
    let leader1 = cr.check_one_leader().await.unwrap();
    cr.disconnect((leader1.id + 2) % node_count as NodeId).await;
    cr.disconnect((leader1.id + 3) % node_count as NodeId).await;
    cr.disconnect((leader1.id + 4) % node_count as NodeId).await;

    // submit lots of commands that won't commit
    for _ in 0..50 {
        next_cmd += 1;
        cr.send_cmd(leader1.id, next_cmd).await;
    }

    cr.sleep(ELECTION_TIMEOUT_MS_HIGH).await;

    // Disconnect old leader and its follower
    cr.disconnect(leader1.id).await;
    cr.disconnect((leader1.id + 1) % node_count as NodeId).await;

    // allow other partition to recover
    cr.connect((leader1.id + 2) % node_count as NodeId).await;
    cr.connect((leader1.id + 3) % node_count as NodeId).await;
    cr.connect((leader1.id + 4) % node_count as NodeId).await;

    // lots of successful commands to new group.
    for _ in 0..50 {
        next_cmd += 1;
        cr.try_to_commit(next_cmd).await.unwrap();
    }

    // now another partitioned leader and one follower
    let leader2 = cr.check_one_leader().await.unwrap();
    let mut other_id = (leader1.id + 2) % node_count as NodeId;
    if leader2.id == other_id {
        other_id = (leader1.id + 3) % node_count as NodeId;
    }
    cr.disconnect(other_id).await;

    // lots more commands that won't commit
    for _ in 0..50 {
        next_cmd += 1;
        cr.send_cmd(leader2.id, next_cmd).await;
    }

    cr.sleep(ELECTION_TIMEOUT_MS_HIGH).await;

    // bring original leader back to life,
    for i in 0..node_count as NodeId {
        cr.disconnect(i).await;
    }
    cr.connect(leader1.id).await;
    cr.connect((leader1.id + 1) % node_count as NodeId).await;
    cr.connect(other_id).await;

    // lots of successful commands to new group.
    for _ in 0..50 {
        next_cmd += 1;
        cr.try_to_commit(next_cmd).await.unwrap();
    }

    // now everyone
    for i in 0..node_count as NodeId {
        cr.connect(i).await;
    }

    next_cmd += 1;
    cr.try_to_commit(next_cmd).await.unwrap();

    println!("{:?}", cr.get_cluster_state().await);

    assert!(cr.n_committed(102).await > node_count as u32 / 2);
    assert!(cr.n_committed(103).await == 0);
}
