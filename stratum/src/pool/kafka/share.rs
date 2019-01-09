use std::net::Ipv4Addr;
use std::vec::Vec;

use super::LargeArray;

const FULLNAME_LIMIT: usize = 46;
const SECONDARY: u32 = 29;
const PRIMARY: u32 = 31;

#[repr(i32)]
#[derive(Debug)]
pub enum SubmitResult {
    Reject = 0,
    Accept,
}

fn get_inet_addr(worker_addr: &str) -> u32 {
    let mut addr_port = worker_addr.split(':').collect::<Vec<&str>>();
    let (addr, _port) = (addr_port[0], addr_port[1]);
    let addrs = addr
        .split('.')
        .map(|s| s.parse::<u8>().unwrap())
        .collect::<Vec<u8>>();
    let ip = Ipv4Addr::new(addrs[3], addrs[2], addrs[1], addrs[0]);
    u32::from(ip)
}

fn get_server_id(server_id: &str) -> u16 {
    let splits = server_id.split('-').collect::<Vec<&str>>();
    splits[1].parse::<u16>().unwrap()
}

fn get_fullname(fullname: &str) -> [char; FULLNAME_LIMIT] {
    let mut fullname = fullname.to_string();
    let length = fullname.len();
    if length < FULLNAME_LIMIT {
        fullname.push_str("\0".repeat(FULLNAME_LIMIT - length).as_str())
    }

    let mut result: [char; FULLNAME_LIMIT] = [char::default(); FULLNAME_LIMIT];
    result.copy_from_slice(fullname.chars().collect::<Vec<char>>().as_slice());
    result
}

#[repr(C)]
#[derive(Deserialize, Serialize, Clone)]
pub struct Share {
    pub job_id: u64,
    pub worker_hash_id: i64, // 0
    pub difficulty: u64,
    pub ip: u32,
    pub user_id: i32, // 0
    pub timestamp: u32,
    pub blkbits: u32, // 0
    pub result: i32,
    pub height: i32,
    pub share_diff: u64, // 0
    pub server_id: u16,
    #[serde(with = "LargeArray")]
    pub fullname: [char; FULLNAME_LIMIT],
}

impl Share {
    pub fn new(
        job_id: u64,
        server_id: String,
        worker_addr: String,
        worker_id: usize,
        difficulty: u64,
        fullname: String,
        result: SubmitResult,
        height: i32,
        timestamp: u32,
    ) -> Share {
        Share {
            job_id,
            difficulty,
            timestamp,
            height,

            worker_hash_id: 0,
            user_id: worker_id as i32,
            blkbits: 0,
            share_diff: 0,

            result: result as i32,
            server_id: get_server_id(&server_id),
            ip: get_inet_addr(&worker_addr),
            fullname: get_fullname(&fullname),
        }
    }
}
