use tokio::sync::Semaphore;

use crate::generator::BatchGenerator;
use crate::workload::Payload;

pub trait RemoteFs {
    type ExternalId;
    async fn write(payload: Payload) -> Self::ExternalId;
    async fn read(id: Self::ExternalId, payload: Payload);
    async fn delete(id: Self::ExternalId);
}

pub struct Driver<Fs: RemoteFs> {
    concurrency: Semaphore,
    remote: Fs,
    generator: BatchGenerator<Fs::ExternalId>,
}

impl<Fs: RemoteFs> Driver<Fs>
where
    Fs::ExternalId: Clone,
{
    pub async fn run_stresstest(&mut self) {
        let batch = self.generator.generate_actions_batch(100);
    }
}
