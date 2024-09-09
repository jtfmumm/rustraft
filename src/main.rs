use tokio::sync::mpsc;

use rustraft::cluster;

#[tokio::main]
async fn main() {
    let (_requests_tx, requests_rx) = mpsc::channel(32);
    let (replies_tx, _replies_rx) = mpsc::channel(32);
    let mut c = cluster::Cluster::new(3, requests_rx, replies_tx);
    tokio::spawn(async move {
        c.run().await;
    });

    // Send commands to cluster over channel

    loop {}
}
