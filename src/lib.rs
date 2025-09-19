//! The `changes-stream` crate is designed to give you a
//! futures::Stream of CouchDB changes stream events.

use bytes::{buf::IntoIter, Bytes};
use futures_util::stream::Stream;
use log::error;
use serde_json::Value;
use std::{mem::replace, pin::Pin, task::Poll, time::Duration};

mod error;
mod event;
pub use error::Error;
pub use event::{Change, ChangeEvent, Event, FinishedEvent};

/// A structure which implements futures::Stream
pub struct ChangesStream {
    /// metrics bytes counter
    #[cfg(feature = "metrics")]
    bytes: metrics::Counter,
    /// metrics entries counter
    #[cfg(feature = "metrics")]
    entries: metrics::Counter,
    /// Source of http chunks provided by reqwest
    source: Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>,
    /// Buffer of current line and current chunk iterator
    buf: (Vec<u8>, Option<IntoIter<Bytes>>),
}

impl ChangesStream {
    /// Constructs a new `ChangesStream` struct with a post payload
    ///
    /// Takes a single argument, `db`, which represents the
    /// url of the data you wish to stream.
    ///
    /// For example, to create a new `ChangesStream` struct
    /// for the npmjs registry, you would write:
    ///
    /// ```no_run
    /// # use changes_stream2::{ChangesStream, Event};
    /// # use futures_util::stream::StreamExt;
    /// #
    /// # #[tokio::main]
    /// # async fn main() {
    /// #     let url = "https://replicate.npmjs.com/_changes?filter=_selector".to_string();
    /// #     let mut changes = ChangesStream::with_post_payload(url, &serde_json::json!({"selector": { "_id": { "$regex": "^_design/" }}})).await.unwrap();
    /// #     while let Some(event) = changes.next().await {
    /// #         match event {
    /// #             Ok(Event::Change(change)) => println!("Change ({}): {}", change.seq, change.id),
    /// #             Ok(Event::Finished(finished)) => println!("Finished: {}", finished.last_seq),
    /// #             Err(err) => println!("Error: {:?}", err),
    /// #         }
    /// #     }
    /// # }
    /// ```
    pub async fn with_post_payload(db: String, payload: &Value) -> Result<Self, Error> {
        let url = url::Url::parse(&db)?;
        #[cfg(feature = "metrics")]
        let database = regex::Regex::new(r"(?m)[_/]+")
            .unwrap()
            .replace_all(
                &format!("{}_{}", url.host_str().unwrap_or_default(), url.path()),
                "_",
            )
            .to_string();

        let client = reqwest::ClientBuilder::new()
            .connect_timeout(Duration::from_secs(60))
            .read_timeout(Duration::from_secs(60))
            .build()?;
        let res = client.post(url).json(payload).send().await?;
        let status = res.status();
        if !status.is_success() {
            let body = res.text().await.unwrap_or_default();
            return Err(Error::InvalidResponse { status, body });
        }
        let source = Pin::new(Box::new(res.bytes_stream()));

        #[cfg(feature = "metrics")]
        let (bytes, entries) = {
            let bytes_name = "couchdb_changes_bytes_total";
            let entries_name = "couchdb_changes_entries_total";
            metrics::describe_counter!(bytes_name, metrics::Unit::Bytes, "Changes stream bytes");
            metrics::describe_counter!(
                entries_name,
                metrics::Unit::Count,
                "Changes stream entries"
            );
            (
                metrics::counter!(bytes_name, "database" => database.clone()),
                metrics::counter!(entries_name, "database" => database),
            )
        };

        Ok(Self {
            #[cfg(feature = "metrics")]
            bytes,
            #[cfg(feature = "metrics")]
            entries,
            source,
            buf: (Vec::new(), None),
        })
    }

    /// Constructs a new `ChangesStream` struct
    ///
    /// Takes a single argument, `db`, which represents the
    /// url of the data you wish to stream.
    ///
    /// For example, to create a new `ChangesStream` struct
    /// for the npmjs registry, you would write:
    ///
    /// ```no_run
    /// # use changes_stream2::{ChangesStream, Event};
    /// # use futures_util::stream::StreamExt;
    /// #
    /// # #[tokio::main]
    /// # async fn main() {
    /// #     let url = "https://replicate.npmjs.com/_changes".to_string();
    /// #     let mut changes = ChangesStream::new(url).await.unwrap();
    /// #     while let Some(event) = changes.next().await {
    /// #         match event {
    /// #             Ok(Event::Change(change)) => println!("Change ({}): {}", change.seq, change.id),
    /// #             Ok(Event::Finished(finished)) => println!("Finished: {}", finished.last_seq),
    /// #             Err(err) => println!("Error: {:?}", err),
    /// #         }
    /// #     }
    /// # }
    /// ```
    pub async fn new(db: String) -> Result<Self, Error> {
        Self::with_post_payload(db, &serde_json::json!({})).await
    }
}

impl Stream for ChangesStream {
    type Item = Result<Event, Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        'main: loop {
            if self.buf.1.is_none() {
                match Stream::poll_next(self.source.as_mut(), cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Ready(Some(Ok(chunk))) => {
                        #[cfg(feature = "metrics")]
                        self.bytes.increment(chunk.len() as u64);

                        self.buf.1 = Some(chunk.into_iter())
                    }
                    Poll::Ready(Some(Err(err))) => {
                        error!("Error getting next chunk: {:?}", err);
                        return Poll::Ready(None);
                    }
                }
            } else {
                let (line, chunk_iter) = &mut self.buf;
                let iter = chunk_iter.as_mut().unwrap();

                loop {
                    if let Some(byte) = iter.next() {
                        if byte == 0x0A {
                            // Found '\n', end of line
                            break;
                        }
                        line.push(byte);
                    } else {
                        // We need another chunk to fill the line
                        *chunk_iter = None;
                        continue 'main;
                    }
                }

                let line = replace(line, Vec::with_capacity(line.len() * 2));
                if line.len() < 14 {
                    // skip prologue, epilogue and empty lines (continuous mode)
                    continue;
                }

                let mut len = line.len();
                if line[len - 1] == 0x0D {
                    // 0x0D is '\r'. CouchDB >= 2.0 sends "\r\n"
                    len -= 1;
                }
                if line[len - 1] == 0x2C {
                    // 0x2C is ','
                    len -= 1;
                }

                let result = serde_json::from_slice::<ChangeEvent>(&line[..len])
                    .map(Event::Change)
                    .or_else(|error| {
                        serde_json::from_slice::<FinishedEvent>(&line[..len])
                            .map(Event::Finished)
                            .map_err(|_err| Error::ParsingFailed {
                                error,
                                json: String::from_utf8(line).unwrap_or_default(),
                            })
                    });

                #[cfg(feature = "metrics")]
                self.entries.increment(1);

                return Poll::Ready(Some(result));
            }
        }
    }
}
