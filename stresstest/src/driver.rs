use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::generator::{BatchGenerator, GeneratorAction};
use crate::raw_seaweed::SeaweedClient;

pub struct Driver {
    pub remote: Arc<SeaweedClient>,
    pub generator: BatchGenerator<String>,
}

impl Driver {
    pub async fn run_stresstest(&mut self) {
        for _ in 0..5 {
            let mut batch = self.generator.generate_actions_batch(500);
            println!("got batch");

            let semaphore = Arc::new(Semaphore::new(100));
            let mut joinset = JoinSet::new();
            loop {
                tokio::select! {
                    new_id = joinset.join_next() => {
                        let Some(new_id) = new_id else {continue};
                        let new_id = new_id.unwrap();
                        if let Some((internal_id, external_id)) = new_id {
                            self.generator.register_id(internal_id, external_id);
                        }
                    }
                    permit = semaphore.clone().acquire_owned() => {
                        let Some(action) = batch.pop() else {
                            let new_ids = joinset.join_all().await;
                            for new_id in new_ids {
                                if let Some((internal_id, external_id)) = new_id {
                                    self.generator.register_id(internal_id, external_id);
                                }
                            }
                            break;
                        };

                        let remote = Arc::clone(&self.remote);
                        let task = async move {
                            let new_id = match action {
                                GeneratorAction::Write(internal_id, payload) => {
                                    let external_id = remote.write(payload).await;
                                    Some((internal_id, external_id))
                                }
                                GeneratorAction::Read(external_id, payload) => {
                                    remote.read(external_id, payload).await;
                                    None
                                }
                                GeneratorAction::Delete(external_id) => {
                                    remote.delete(external_id).await;
                                    None
                                }
                            };
                            drop(permit);
                            new_id
                        };
                        joinset.spawn(task);
                    },
                }
            }
        }
    }
}
