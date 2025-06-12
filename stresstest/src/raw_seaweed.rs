use std::convert::Infallible;
use std::io::Read;
use std::task::Poll;

use futures_util::StreamExt as _;
use reqwest::multipart::Part;
use reqwest::{Body, multipart};
use serde::Deserialize;

use crate::workload::Payload;

pub struct SeaweedClient {
    pub master_url: String,
    pub client: reqwest::Client,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssignResponse {
    fid: String,
    public_url: String,
}

#[derive(Deserialize)]
struct LookupResponse {
    locations: Vec<LookupLocation>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LookupLocation {
    public_url: String,
}

impl SeaweedClient {
    pub async fn write(&self, mut payload: Payload) -> String {
        let assign_url = format!("{}/dir/assign", self.master_url);
        let assign: AssignResponse = self
            .client
            .get(assign_url)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let post_url = format!("http://{}/{}", assign.public_url, assign.fid);
        let stream = futures_util::stream::poll_fn(move |_| {
            if payload.len == 0 {
                return Poll::Ready(None);
            }
            let mut read_buf = Vec::with_capacity(1024 * 1024);
            read_buf.resize(1024 * 1024, 0);
            let read_len = payload.read(&mut read_buf).unwrap();
            read_buf.truncate(read_len);

            Poll::Ready(Some(Ok::<_, Infallible>(read_buf)))
        });
        self.client
            .put(post_url)
            .body(Body::wrap_stream(stream))
            .send()
            .await
            .unwrap();

        assign.fid
    }

    async fn lookup_volume_url(&self, id: &str) -> String {
        let lookup_url = format!("{}/dir/lookup?volumeId={id}", self.master_url);
        let lookup: LookupResponse = self
            .client
            .get(lookup_url)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let first_volume = lookup.locations.into_iter().next().unwrap();
        first_volume.public_url
    }

    pub async fn read(&self, id: String, mut payload: Payload) {
        let volume_url = self.lookup_volume_url(&id).await;
        let file_url = format!("http://{volume_url}/{id}");
        let mut stream = self
            .client
            .get(file_url)
            .send()
            .await
            .unwrap()
            .bytes_stream();

        let mut comparison_buffer = Vec::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            comparison_buffer.resize(chunk.len(), 0);
            let read_len = payload.read(&mut comparison_buffer).unwrap();
            assert_eq!(read_len, chunk.len());
            assert_eq!(chunk, &comparison_buffer);
        }
    }

    pub async fn delete(&self, id: String) {
        let volume_url = self.lookup_volume_url(&id).await;
        let file_url = format!("http://{volume_url}/{id}");
        self.client.delete(file_url).send().await.unwrap();
    }
}
