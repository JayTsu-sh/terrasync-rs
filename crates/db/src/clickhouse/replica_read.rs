//! 保留原有一致性设置；副本尚未到达已确认 quorum 时切换副本重试读取。
use clickhouse::{
    RowOwned, RowRead,
    error::{Error, Result},
    query::Query,
};
use std::{
    future::Future,
    time::{Duration, Instant},
};
use tokio::time::sleep;
use tracing::warn;

pub(crate) trait QuorumRead {
    async fn fetch_quorum_one<T: RowOwned + RowRead>(self) -> Result<T>;
    async fn fetch_quorum_all<T: RowOwned + RowRead>(self) -> Result<Vec<T>>;
}

impl QuorumRead for Query {
    async fn fetch_quorum_one<T: RowOwned + RowRead>(self) -> Result<T> {
        read(self, Query::fetch_one::<T>).await
    }
    async fn fetch_quorum_all<T: RowOwned + RowRead>(self) -> Result<Vec<T>> {
        read(self, Query::fetch_all::<T>).await
    }
}

async fn read<T, F, Fut>(query: Query, mut operation: F) -> Result<T>
where
    F: FnMut(Query) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let started = Instant::now();
    let mut retrying = false;
    loop {
        let request = if retrying {
            query.clone().with_option("load_balancing", "random")
        } else {
            query.clone()
        };
        match operation(request).await {
            Err(Error::BadResponse(ref message))
                if message.contains("REPLICA_IS_NOT_IN_QUORUM") && started.elapsed() < Duration::from_secs(30) =>
            {
                if !retrying {
                    warn!(
                        "Replica has not reached the acknowledged quorum; retrying the consistent read on another replica"
                    );
                }
                retrying = true;
                sleep(Duration::from_millis(50)).await;
            }
            result => return result,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use clickhouse::{Client, Compression};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        task::JoinHandle,
    };

    async fn server(responses: Vec<(u16, Vec<u8>)>) -> (String, JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(responses.len());
            for (status, body) in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::with_capacity(4096);
                let mut chunk = [0; 4096];
                loop {
                    let count = socket.read(&mut chunk).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&chunk[..count]);
                    if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|value| value.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + length {
                            requests.push(headers.lines().next().unwrap().to_owned());
                            break;
                        }
                    }
                }
                let header = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(header.as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            }
            requests
        });
        (format!("http://{address}"), task)
    }

    #[tokio::test]
    async fn retries_replica_lag_without_weakening_consistency() {
        let (url, server) = server(vec![
            (500, b"Code: 289. REPLICA_IS_NOT_IN_QUORUM".to_vec()),
            (200, 42_u64.to_le_bytes().to_vec()),
        ])
        .await;
        let client = Client::default()
            .with_url(url)
            .with_validation(false)
            .with_compression(Compression::None)
            .with_option("select_sequential_consistency", "1")
            .with_option("load_balancing", "in_order");
        let result = client.query("SELECT 42").fetch_quorum_one::<u64>().await;
        if result.is_err() {
            server.abort();
        }
        assert_eq!(result.unwrap(), 42);
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .all(|request| request.contains("select_sequential_consistency=1"))
        );
        assert!(requests[1].contains("load_balancing=random"));
    }

    #[tokio::test]
    async fn other_database_failures_are_not_retried() {
        let (url, server) = server(vec![(403, b"permission denied".to_vec())]).await;
        let client = Client::default()
            .with_url(url)
            .with_validation(false)
            .with_compression(Compression::None);
        assert!(client.query("SELECT 42").fetch_quorum_one::<u64>().await.is_err());
        assert_eq!(server.await.unwrap().len(), 1);
    }
}
