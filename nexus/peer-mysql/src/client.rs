use std::sync::Arc;

use futures::StreamExt;
use mysql_async::{self, prelude::Queryable};
use tokio::{spawn, sync::mpsc};

pub enum Response {
    Row(mysql_async::Row),
    Schema(Arc<[mysql_async::Column]>),
    Err(mysql_async::Error),
}

pub struct Message {
    pub query: String,
    pub response: mpsc::Sender<Response>,
}

#[derive(Clone)]
pub struct MyClient {
    pub chan: mpsc::Sender<Message>,
}

impl MyClient {
    pub async fn new(opts: mysql_async::Opts) -> mysql_async::Result<MyClient> {
        let mut conn = mysql_async::Conn::new(opts).await?;
        let (send, mut recv) = mpsc::channel(1);
        spawn(async move {
            while let Some(Message { query, response }) = recv.recv().await {
                match conn.query_stream(query).await {
                    Ok(stream) => {
                        response.send(Response::Schema(stream.columns())).await.ok();
                        stream
                            .for_each_concurrent(1, async |row| {
                                response
                                    .send(match row {
                                        Ok(row) => Response::Row(row),
                                        Err(err) => Response::Err(err),
                                    })
                                    .await
                                    .ok();
                            })
                            .await;
                    }
                    Err(e) => {
                        response.send(Response::Err(e)).await.ok();
                    }
                }
            }
        });

        Ok(MyClient { chan: send })
    }
}
