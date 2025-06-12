use std::time::Duration;

use crate::raw_seaweed::SeaweedClient;
use crate::stresstest::perform_stresstest;
use crate::workload::Workload;

mod raw_seaweed;
mod stresstest;
mod workload;

#[tokio::main]
async fn main() {
    // docker run -p9333:9333 -p8080:8080 --rm -it chrislusf/seaweedfs server
    let remote = SeaweedClient {
        master_url: "http://localhost:9333".into(),
        client: reqwest::Client::new(),
    };
    let workload = Workload::builder()
        .concurrency(32)
        .size_distribution(16 * 1024, 1024 * 1024) // p50 = 16K, p99 = 1M
        .action_weights(98, 2, 0)
        .build();

    perform_stresstest(remote, vec![workload], Duration::from_secs(2))
        .await
        .unwrap();
}
