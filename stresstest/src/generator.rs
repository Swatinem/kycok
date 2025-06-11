use indexmap::IndexMap;

use crate::workload::{DynWorkload, Payload, WorkloadAction};

#[derive(Eq, Hash, PartialEq)]
pub struct InternalId(u64);

pub struct BatchGenerator<ExternalId> {
    workload: Box<dyn DynWorkload>,
    existing_files: IndexMap<InternalId, ExternalId>,
}

impl<ExternalId: Clone> BatchGenerator<ExternalId> {
    pub fn generate_actions_batch(&mut self, n: usize) -> Vec<GeneratorAction<ExternalId>> {
        let mut actions = Vec::with_capacity(n);
        let mut read_entries = Vec::with_capacity(n);
        loop {
            let candidate_action = self.workload.sample_action();
            match candidate_action {
                WorkloadAction::Write => {
                    let (seed, payload) = self.workload.sample_payload();
                    actions.push(GeneratorAction::Write(InternalId(seed), payload));
                }
                action if !self.existing_files.is_empty() => {
                    let idx = self.workload.sample_readback(self.existing_files.len());
                    let idx = self.existing_files.len() - idx;
                    // By using `remove` for both reads and deletes, we can guarantee that
                    // we won’t be reading deleted files.
                    // But it also means we do not read files twice within one batch.
                    let (internal, external) = self.existing_files.shift_remove_index(idx).unwrap();

                    if matches!(action, WorkloadAction::Read) {
                        let payload = self.workload.get_payload(internal.0);
                        actions.push(GeneratorAction::Read(external.clone(), payload));
                        read_entries.push((internal, external));
                    } else {
                        actions.push(GeneratorAction::Delete(external))
                    }
                }
                _ => {}
            }

            if actions.len() == n {
                return actions;
            }
        }
    }

    pub fn register_id(&mut self, internal: InternalId, external: ExternalId) {
        self.existing_files.insert(internal, external);
    }
}

pub enum GeneratorAction<ExternalId> {
    Write(InternalId, Payload),
    Read(ExternalId, Payload),
    Delete(ExternalId),
}
