use std::io;

use rand::rngs::SmallRng;
use rand::{Rng as _, RngCore, SeedableRng};
use rand_distr::weighted::WeightedIndex;
use rand_distr::{Distribution, Zipf};

#[derive(Clone, Copy)]
pub enum WorkloadAction {
    Write,
    Read,
    Delete,
}

pub trait DynWorkload {
    fn get_payload(&self, seed: u64) -> Payload;
    fn sample_payload(&mut self) -> (u64, Payload);
    fn sample_action(&mut self) -> WorkloadAction;
    fn sample_readback(&mut self, len: usize) -> usize;
}

pub struct Workload<S> {
    /// The RNG driving all our distributions.
    pub rng: SmallRng,
    /// A distribution that generates payload sizes for the `write` action.
    pub size_distribution: S,
    /// A distribution that generates actions, such as write/read/delete.
    pub action_distribution: WeightedIndex<u8>,
}

impl<S> DynWorkload for Workload<S>
where
    S: Distribution<f64>,
{
    fn get_payload(&self, seed: u64) -> Payload {
        let mut rng = SmallRng::seed_from_u64(seed);
        let len = self.size_distribution.sample(&mut rng) as u64;

        Payload { len, rng }
    }

    fn sample_payload(&mut self) -> (u64, Payload) {
        let seed = self.rng.next_u64();
        (seed, self.get_payload(seed))
    }

    fn sample_action(&mut self) -> WorkloadAction {
        const ACTIONS: &[WorkloadAction] = &[
            WorkloadAction::Write,
            WorkloadAction::Read,
            WorkloadAction::Delete,
        ];
        ACTIONS[self.action_distribution.sample(&mut self.rng)]
    }

    fn sample_readback(&mut self, len: usize) -> usize {
        let zipf = Zipf::new(len as f64, 2.0).unwrap();
        self.rng.sample(zipf) as usize
    }
}

pub struct Payload {
    pub len: u64,
    pub rng: SmallRng,
}

impl io::Read for Payload {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len_to_fill = (buf.len() as u64).min(self.len) as usize;

        let fill_buf = &mut buf[..len_to_fill];
        self.rng.fill_bytes(fill_buf);

        self.len -= len_to_fill as u64;
        Ok(len_to_fill)
    }
}

#[cfg(test)]
mod tests {
    use rand_distr::LogNormal;

    use super::*;

    #[test]
    fn generates_payloads() {
        let size_distribution = LogNormal::from_mean_cv(1000.0, 2.5).unwrap();
        let action_distribution = WeightedIndex::new(&[98, 2, 0]).unwrap();

        let mut template: Box<dyn DynWorkload> = Box::new(Workload {
            rng: SmallRng::seed_from_u64(0),
            size_distribution,
            action_distribution,
        });

        for seed in 0..100 {
            let payload = template.get_payload(seed);
            dbg!(payload.len);
        }
    }
}
