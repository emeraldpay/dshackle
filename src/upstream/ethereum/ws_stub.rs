// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A scripted Ethereum node speaking JSON-RPC over WebSocket, for the tests
//! of everything built on a node's `eth_subscribe`.

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;

/// How long [`StubNode::requests`] waits for the expected requests: far above
/// any local round trip, so a timeout means the request is never coming.
const REQUEST_WAIT: Duration = Duration::from_secs(5);

/// What a test tells every open connection of the node to do.
#[derive(Clone)]
enum Command {
    /// Push the payload to each active subscription.
    Notify(Value),
    /// Close the connection without a goodbye.
    Disconnect,
}

/// The node: accepts any number of connections, grants every `eth_subscribe`
/// a subscription id, honors `eth_unsubscribe`, and records each request it
/// receives.
pub(crate) struct StubNode {
    url: String,
    requests: Arc<Mutex<Vec<Value>>>,
    commands: broadcast::Sender<Command>,
}

impl StubNode {
    pub(crate) async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let commands = broadcast::channel(64).0;

        let node_requests = Arc::clone(&requests);
        let node_commands = commands.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve(
                    stream,
                    Arc::clone(&node_requests),
                    node_commands.subscribe(),
                ));
            }
        });

        Self {
            url,
            requests,
            commands,
        }
    }

    pub(crate) fn url(&self) -> String {
        self.url.clone()
    }

    /// Push `result` as an `eth_subscription` notification to every active
    /// subscription.
    pub(crate) fn notify(&self, result: Value) {
        let _ = self.commands.send(Command::Notify(result));
    }

    /// Drop every open connection, as a crashed node or a broken network would.
    pub(crate) fn disconnect(&self) {
        let _ = self.commands.send(Command::Disconnect);
    }

    /// The `params` of each `method` request received so far, waiting until
    /// there are at least `count` of them.
    pub(crate) async fn requests(&self, method: &str, count: usize) -> Vec<Value> {
        let received = || -> Vec<Value> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request["method"] == method)
                .map(|request| request["params"].clone())
                .collect()
        };
        let enough = async {
            loop {
                let found = received();
                if found.len() >= count {
                    return found;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        match tokio::time::timeout(REQUEST_WAIT, enough).await {
            Ok(found) => found,
            Err(_) => panic!("expected {count} {method} request(s), got {:?}", received()),
        }
    }
}

async fn serve(
    stream: TcpStream,
    requests: Arc<Mutex<Vec<Value>>>,
    mut commands: broadcast::Receiver<Command>,
) {
    let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
        return;
    };
    let mut subscriptions: HashSet<String> = HashSet::new();
    // Numbered from the start on every connection, as some real nodes do: an
    // id is then no proof of which connection a subscription was made on.
    let mut next_subscription = 1u32;
    loop {
        tokio::select! {
            incoming = ws.next() => {
                let Some(Ok(Message::Text(text))) = incoming else {
                    return;
                };
                let request: Value = serde_json::from_str(&text).unwrap();
                let result = match request["method"].as_str() {
                    Some("eth_subscribe") => {
                        let id = format!("0x{next_subscription:x}");
                        next_subscription += 1;
                        subscriptions.insert(id.clone());
                        json!(id)
                    }
                    Some("eth_unsubscribe") => {
                        let id = request["params"][0].as_str().unwrap_or_default();
                        json!(subscriptions.remove(id))
                    }
                    _ => Value::Null,
                };
                let response = json!({"jsonrpc": "2.0", "id": request["id"], "result": result});
                requests.lock().unwrap().push(request);
                if ws.send(Message::Text(response.to_string().into())).await.is_err() {
                    return;
                }
            }
            command = commands.recv() => match command {
                Ok(Command::Notify(result)) => {
                    for id in &subscriptions {
                        let notification = json!({
                            "jsonrpc": "2.0",
                            "method": "eth_subscription",
                            "params": {"subscription": id, "result": result},
                        });
                        if ws.send(Message::Text(notification.to_string().into())).await.is_err() {
                            return;
                        }
                    }
                }
                Ok(Command::Disconnect) | Err(_) => return,
            }
        }
    }
}
