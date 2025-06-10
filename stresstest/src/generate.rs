use std::io;

use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use rand_distr::Distribution;

trait DynWorkload {
    fn sample_payload(&self, seed: u64) -> Payload;
}

pub struct Workload<S, A> {
    /// A distribution that generates payload sizes.
    size_distribution: S,
    /// A distribution that generates actions, such as write/read/delete.
    action_distribution: A,
}

impl<S, A> DynWorkload for Workload<S, A>
where
    S: Distribution<f64>,
{
    fn sample_payload(&self, seed: u64) -> Payload {
        let mut rng = SmallRng::seed_from_u64(seed);
        let len = self.size_distribution.sample(&mut rng) as u64;

        Payload { len, rng }
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
        let template: Box<dyn DynWorkload> = Box::new(Workload {
            size_distribution,
            action_distribution: (),
        });

        for seed in 0..10 {
            let payload = template.sample_payload(seed);
            dbg!(payload.len);
        }
    }
}
