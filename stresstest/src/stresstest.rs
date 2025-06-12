use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;

use bytesize::ByteSize;
use sketches_ddsketch::DDSketch;
use tokio::sync::Semaphore;

use crate::raw_seaweed::SeaweedClient;
use crate::workload::{Action, Workload};

pub async fn perform_stresstest(
    remote: SeaweedClient,
    workloads: Vec<Workload>,
    duration: Duration,
) -> Result<()> {
    let remote = Arc::new(remote);
    // run the workloads concurrently
    let tasks: Vec<_> = workloads.into_iter().map(|workload| {
        let remote = Arc::clone(&remote);
         tokio::spawn(async move {
            let concurrency = workload.config.concurrency;
            let semaphore = Arc::new(Semaphore::new(concurrency));
            let deadline = tokio::time::Instant::now() + duration;

            let workload = Arc::new(Mutex::new(workload));

            let task_durations = Arc::new(Mutex::new(DDSketch::default()));

            // See <https://docs.rs/tokio/latest/tokio/time/struct.Sleep.html#examples>
            let sleep = tokio::time::sleep_until(deadline);
            tokio::pin!(sleep);

            loop {
                if deadline.elapsed() > Duration::ZERO {
                    break;
                }
                tokio::select! {
                    permit = semaphore.clone().acquire_owned() => {
                        let workload = Arc::clone(&workload);
                        let remote = Arc::clone(&remote);
                        let task_durations = Arc::clone(&task_durations);
                        let task_start = Instant::now();

                        let task = async move {
                            let action = workload.lock().unwrap().next_action();
                            match action {
                                Action::Write(internal_id, payload) => {
                                    let external_id = remote.write(payload).await;
                                    workload.lock().unwrap().push_file(internal_id, external_id);
                                },
                                Action::Read(internal_id, external_id, payload) => {
                                    remote.read(&external_id, payload).await;
                                    workload.lock().unwrap().push_file(internal_id, external_id);
                                },
                                Action::Delete(external_id) => {
                                    remote.delete(external_id).await;
                                },
                            }

                            task_durations.lock().unwrap().add(task_start.elapsed().as_secs_f64());

                            drop(permit);
                        };
                        tokio::spawn(task);
                    }
                    _ = &mut sleep => {
                        break;
                    }
                }
            }

            let task_durations: DDSketch = {
                let mut task_durations = task_durations.lock().unwrap();
                std::mem::take(&mut task_durations)
            };

            // by acquiring *all* the semaphores, we essentially wait for all outstanding tasks to finish
            let _permits = semaphore.acquire_many(concurrency as u32).await;

            let workload = Arc::try_unwrap(workload).map_err(|_|()).unwrap().into_inner().unwrap();
            (workload, task_durations)
        })
    }).collect();

    let finished_tasks = futures::future::join_all(tasks).await;

    for (i, task) in finished_tasks.into_iter().enumerate() {
        let (workload, task_durations) = task.unwrap();

        let config = workload.config;
        let concurrency = config.concurrency;
        // let sum_ops = config.write_weight + config.read_weight + config.delete_weight;
        let ops = task_durations.count();
        let ops_ps = ops as f32 / duration.as_secs() as f32;
        println!("# Workload {i} (concurrency: {concurrency})");
        println!(
            "  file sizes {}-{}",
            ByteSize::b(config.p50_size),
            ByteSize::b(config.p99_size)
        );
        println!("=> {ops} operations, {ops_ps:.2} ops/s");

        let avg = Duration::from_secs_f64(task_durations.sum().unwrap() / ops as f64);
        let p50 = Duration::from_secs_f64(task_durations.quantile(0.5).unwrap().unwrap());
        let p90 = Duration::from_secs_f64(task_durations.quantile(0.9).unwrap().unwrap());
        let p99 = Duration::from_secs_f64(task_durations.quantile(0.99).unwrap().unwrap());
        println!("  avg: {avg:.2?}; p50: {p50:.2?}; p90: {p90:.2?}; p99: {p99:.2?}");
    }

    Ok(())
}
