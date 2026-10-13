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

//! A scripted Esplora HTTP server, for the tests of everything built on the
//! Esplora client.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use warp::Filter;
use warp::http::{Response, StatusCode};

/// What the server answers for one path.
#[derive(Clone)]
enum Answer {
    Json(String),
    Status(u16),
}

/// One request the server received.
#[derive(Debug, Clone)]
pub(crate) struct StubRequest {
    pub(crate) path: String,
    pub(crate) authorization: Option<String>,
}

/// The server: answers a path with what the test scripted for it and records
/// every request. A path nothing was scripted for answers an empty JSON list,
/// which is how Esplora describes an address it has never seen.
pub(crate) struct StubEsplora {
    url: String,
    answers: Arc<Mutex<HashMap<String, Answer>>>,
    requests: Arc<Mutex<Vec<StubRequest>>>,
}

impl StubEsplora {
    pub(crate) async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let answers: Arc<Mutex<HashMap<String, Answer>>> = Arc::default();
        let requests: Arc<Mutex<Vec<StubRequest>>> = Arc::default();

        let served_answers = Arc::clone(&answers);
        let served_requests = Arc::clone(&requests);
        let route = warp::path::full()
            .and(warp::header::optional::<String>("authorization"))
            .map(
                move |path: warp::path::FullPath, authorization: Option<String>| {
                    let path = path.as_str().to_string();
                    let answer = served_answers.lock().unwrap().get(&path).cloned();
                    served_requests.lock().unwrap().push(StubRequest {
                        path,
                        authorization,
                    });
                    match answer {
                        Some(Answer::Json(body)) => Response::new(body),
                        Some(Answer::Status(code)) => {
                            let mut response = Response::new(String::new());
                            *response.status_mut() = StatusCode::from_u16(code).unwrap();
                            response
                        }
                        None => Response::new("[]".to_string()),
                    }
                },
            );
        tokio::spawn(warp::serve(route).incoming(listener).run());

        Self {
            url,
            answers,
            requests,
        }
    }

    /// API root to point a client at.
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    /// Answer `path` with the JSON `body`.
    pub(crate) fn respond(&self, path: &str, body: &str) {
        self.answers
            .lock()
            .unwrap()
            .insert(path.to_string(), Answer::Json(body.to_string()));
    }

    /// Answer `path` with the HTTP `status` and no body.
    pub(crate) fn fail(&self, path: &str, status: u16) {
        self.answers
            .lock()
            .unwrap()
            .insert(path.to_string(), Answer::Status(status));
    }

    /// Every request received so far, in arrival order.
    pub(crate) fn requests(&self) -> Vec<StubRequest> {
        self.requests.lock().unwrap().clone()
    }
}
