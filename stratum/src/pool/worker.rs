// Copyright 2018 Blade M. Doyle
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

//! Mining Stratum Worker
//!
//! A single mining worker (the pool manages a vec of Workers)
//!

use bufstream::BufStream;
use serde_json;
use serde_json::Value;
use std::collections::HashSet;
use std::net::TcpStream;

use pool::logger::LOGGER;
use pool::proto::RpcRequest;
use pool::proto::{JobTemplate, LoginParams, StratumProtocol, SubmitParams, WorkerStatus};

// ----------------------------------------
// Worker Object - a connected stratum client - a miner

fn validate_legal_string(check: &str, legal: &str) -> bool {
    let legal = legal.replace(" ", "");
    let legal = legal
        .split("")
        .filter(|x| !x.is_empty())
        .collect::<HashSet<_>>();
    let check = check.replace(" ", "");
    let check = check
        .split("")
        .filter(|x| !x.is_empty())
        .collect::<HashSet<_>>();
    !(check.difference(&legal).collect::<HashSet<_>>().len() > 0)
}

// Validate fullname
fn validate_fullname(login_params: &mut LoginParams) -> bool {
    let splits = login_params
        .login
        .split('.')
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
    let username: &str;
    let workername: &str;
    let mut need_reconcat = false;
    if splits.len() >= 2 {
        username = splits[0].as_str();
        workername = splits[1].as_str();
    } else {
        need_reconcat = true;
        username = splits[0].as_str();
        workername = "__default__";
    }
    if validate_username(username) && validate_workername(workername) {
        if need_reconcat {
            login_params.login = format!("{}.{}", username, workername);
        }
        true
    } else {
        false
    }
}

fn validate_username(username: &str) -> bool {
    if username.is_empty() || username.len() > 20 {
        false
    } else {
        let legal = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890_";
        validate_legal_string(username, legal)
    }
}

fn validate_workername(workername: &str) -> bool {
    if workername.is_empty() || workername.len() > 18 {
        false
    } else {
        let legal = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890_.-";
        validate_legal_string(workername, legal)
    }
}

#[derive(Debug)]
pub struct WorkerConfig {}

pub struct Worker {
    pub id: usize,
    login: Option<LoginParams>,
    stream: BufStream<TcpStream>,
    protocol: StratumProtocol,
    error: bool,
    authenticated: bool,
    pub status: WorkerStatus,       // Runing totals
    pub block_status: WorkerStatus, // Totals for current block
    shares: Vec<SubmitParams>,
    pub needs_job: bool,
    pub addr: String,
}

impl Worker {
    /// Creates a new Stratum Worker.
    pub fn new(id: usize, addr: String, stream: BufStream<TcpStream>) -> Worker {
        Worker {
            id: id,
            login: None,
            stream: stream,
            protocol: StratumProtocol::new(),
            error: false,
            authenticated: false,
            status: WorkerStatus::new(id.to_string()),
            block_status: WorkerStatus::new(id.to_string()),
            shares: Vec::new(),
            needs_job: true,
            addr: addr,
        }
    }

    /// Is the worker in error state?
    pub fn error(&self) -> bool {
        return self.error;
    }

    pub fn set_error(&mut self) {
        self.error = true;
    }

    /// get the id
    pub fn id(&self) -> usize {
        return self.id;
    }

    /// Get worker login
    pub fn login(&self) -> String {
        match self.login {
            None => "None.__default__".to_string(),
            Some(ref login) => login.login.clone(),
        }
    }

    /// Set job difficulty
    pub fn set_difficulty(&mut self, new_difficulty: u64) {
        self.status.difficulty = new_difficulty;
    }

    /// Set job height
    pub fn set_height(&mut self, new_height: u64) {
        self.status.height = new_height;
    }

    // XXX TODO: I may need seprate send_job_request() and send_jog_response() methods?
    /// Send a job to the worker
    pub fn send_job(&mut self, job: &mut JobTemplate) -> Result<(), String> {
        trace!(LOGGER, "Worker {} - Sending a job downstream", self.id);
        // Set the difficulty
        job.difficulty = self.status.difficulty;
        self.needs_job = false;
        let job_value = serde_json::to_value(job).unwrap();
        return self.protocol.send_response(
            &mut self.stream,
            "getjobtemplate".to_string(),
            job_value,
            self.id,
        );
    }

    /// Send worker mining status
    pub fn send_status(&mut self, status: WorkerStatus) -> Result<(), String> {
        trace!(LOGGER, "Worker {} - Sending worker status", self.id);
        let status_value = serde_json::to_value(status).unwrap();
        return self.protocol.send_response(
            &mut self.stream,
            "status".to_string(),
            status_value,
            self.id,
        );
    }

    /// Send OK Response
    pub fn send_ok(&mut self, method: String) -> Result<(), String> {
        trace!(LOGGER, "Worker {} - sending OK Response", self.id);
        return self.protocol.send_response(
            &mut self.stream,
            method,
            serde_json::to_value("ok".to_string()).unwrap(),
            self.id,
        );
    }

    /// Return any pending shares from this worker
    pub fn get_shares(&mut self) -> Result<Option<Vec<SubmitParams>>, String> {
        if self.shares.len() > 0 {
            trace!(
                LOGGER,
                "Worker {} - Getting {} shares",
                self.id,
                self.shares.len()
            );
            let current_shares = self.shares.clone();
            self.shares = Vec::new();
            return Ok(Some(current_shares));
        }
        return Ok(None);
    }

    /// Get and process messages from the connected worker
    // Method to handle requests from the downstream worker
    pub fn process_messages(&mut self) -> Result<(), String> {
        // XXX TODO: With some reasonable rate limiting (like N message per pass)
        // Read some messages from the upstream
        // Handle each request
        match self.protocol.get_message(&mut self.stream) {
            Ok(rpc_msg) => {
                match rpc_msg {
                    Some(message) => {
                        trace!(LOGGER, "Worker {} - Got Message: {:?}", self.id, message);
                        // let v: Value = serde_json::from_str(&message).unwrap();
                        let req: RpcRequest = match serde_json::from_str(&message) {
                            Ok(r) => r,
                            Err(e) => {
                                self.error = true;
                                // XXX TODO: Invalid request
                                return Err(e.to_string());
                            }
                        };
                        trace!(
                            LOGGER,
                            "Worker {} - Received request type: {}",
                            self.id,
                            req.method
                        );
                        match req.method.as_str() {
                            "login" => {
                                debug!(LOGGER, "Worker {} - Accepting Login request", self.id);
                                let params: Value = match req.params {
                                    Some(p) => p,
                                    None => {
                                        self.error = true;
                                        // XXX TODO: Invalid request
                                        return Err("invalid request".to_string());
                                    }
                                };
                                let mut login_params: LoginParams =
                                    match serde_json::from_value(params) {
                                        Ok(p) => p,
                                        Err(e) => {
                                            self.error = true;
                                            // XXX TODO: Invalid request
                                            return Err(e.to_string());
                                        }
                                    };
                                // XXX TODO: Validate the login - is it a valid grin wallet address?
                                if validate_fullname(&mut login_params) {
                                    self.login = Some(login_params);
                                    // We accepted the login, send ok result
                                    self.send_ok(req.method);
                                } else {
                                    warn!(
                                        LOGGER,
                                        "Worker {} - Is Invalid Name.", login_params.login
                                    );
                                    return Err("invalid worker name".to_string());
                                }
                            }
                            "getjobtemplate" => {
                                debug!(LOGGER, "Worker {} - Accepting request for job", self.id);
                                self.needs_job = true;
                            }
                            "submit" => {
                                debug!(LOGGER, "Worker {} - Accepting share", self.id);
                                match serde_json::from_value(req.params.unwrap()) {
                                    Result::Ok(share) => {
                                        self.shares.push(share);
                                    }
                                    Result::Err(err) => {}
                                };
                            }
                            "status" => {
                                trace!(LOGGER, "Worker {} - Accepting status request", self.id);
                                let status = self.status.clone();
                                self.send_status(status);
                            }
                            "keepalive" => {
                                trace!(LOGGER, "Worker {} - Accepting keepalive request", self.id);
                                self.send_ok(req.method);
                            }
                            _ => {
                                warn!(
                                    LOGGER,
                                    "Worker {} - Unknown request: {}",
                                    self.id,
                                    req.method.as_str()
                                );
                                self.error = true;
                                return Err("Unknown request".to_string());
                            }
                        };
                    }
                    None => {} // Not an error, just no messages for us right now
                }
            }
            Err(e) => {
                self.error = true;
                return Err(e.to_string());
            }
        }
        return Ok(());
    }
}
