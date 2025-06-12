use std::sync::Arc;

use rand::SeedableRng;
use rand::rngs::SmallRng;
use rand_distr::LogNormal;
use rand_distr::weighted::WeightedIndex;

use crate::driver::Driver;
use crate::generator::BatchGenerator;
use crate::raw_seaweed::SeaweedClient;
use crate::workload::{DynWorkload, Workload};

mod driver;
mod generator;
mod raw_seaweed;
mod workload;

#[tokio::main]
async fn main() {
    // docker run -p9333:9333 -p8080:8080 -p 8333:8333 --rm -it chrislusf/seaweedfs server -s3
    let remote = Arc::new(SeaweedClient {
        master_url: "http://localhost:9333".into(),
        client: reqwest::Client::new(),
    });
    let size_distribution = LogNormal::from_mean_cv(1000.0, 2.5).unwrap();
    let action_distribution = WeightedIndex::new(&[98, 2, 0]).unwrap();

    let workload: Box<dyn DynWorkload> = Box::new(Workload {
        rng: SmallRng::seed_from_u64(0),
        size_distribution,
        action_distribution,
    });
    let generator = BatchGenerator {
        workload,
        existing_files: Default::default(),
    };
    let mut driver = Driver { remote, generator };

    driver.run_stresstest().await
}
