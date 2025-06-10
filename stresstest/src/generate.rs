use std::io;

use rand::rngs::SmallRng;
use rand::{Rng as _, RngCore, SeedableRng};
use rand_distr::{Distribution, Zipf};

pub enum Action {
    Write,
    Read,
    Delete,
}

trait DynWorkload {
    fn sample_payload(&mut self) -> Payload;
    fn sample_action(&mut self) -> Action;
    fn sample_readback(&mut self, len: usize) -> usize;
}

pub struct Workload<S, A, R> {
    /// The RNG driving all our distributions.
    rng: SmallRng,
    /// A distribution that generates payload sizes for the `write` action.
    size_distribution: S,
    /// A distribution that generates actions, such as write/read/delete.
    action_distribution: A,
    /// A distribution that picks a previously written file, to either `read` or `delete` it.
    read_distribution: R,
}

impl<S, A, R> DynWorkload for Workload<S, A, R>
where
    S: Distribution<f64>,
{
    fn sample_payload(&mut self) -> Payload {
        let seed = self.rng.next_u64();
        let mut rng = SmallRng::seed_from_u64(seed);
        let len = self.size_distribution.sample(&mut rng) as u64;

        Payload { len, rng }
    }

    fn sample_action(&mut self) -> Action {
        todo!()
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

        Ok(len_to_fill)
    }
}

#[cfg(test)]
mod tests {
    use rand_distr::LogNormal;

    use super::*;

    #[test]
    fn generates_payloads() {
        let size_distribution = LogNormal::from_mean_cv(100.0, 0.5).unwrap();
        let mut template: Box<dyn DynWorkload> = Box::new(Workload {
            rng: SmallRng::seed_from_u64(0),
            size_distribution,
            action_distribution: (),
            read_distribution: (),
        });

        for seed in 0..10 {
            let payload = template.sample_payload();
            dbg!(payload.len);
        }
    }
}
