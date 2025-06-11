use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::generator::{BatchGenerator, GeneratorAction};
use crate::workload::Payload;

pub trait RemoteFs: Send + Sync {
    type ExternalId: Send + 'static;
    async fn write(&self, payload: Payload) -> Self::ExternalId;
    async fn read(&self, id: Self::ExternalId, payload: Payload);
    async fn delete(&self, id: Self::ExternalId);
}

pub struct Driver<Fs: RemoteFs> {
    concurrency: Semaphore,
    remote: Arc<Fs>,
    generator: BatchGenerator<Fs::ExternalId>,
}

impl<Fs: RemoteFs + Send + 'static> Driver<Fs>
where
    Fs::ExternalId: Clone,
{
    pub async fn run_stresstest(&mut self) {
        let mut batch = self.generator.generate_actions_batch(100);

        let semaphore = Arc::new(Semaphore::new(20));
        let mut joinset = JoinSet::new();
        loop {
            let permit = semaphore.clone().acquire_owned().await.unwrap();

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
        }
    }
}
