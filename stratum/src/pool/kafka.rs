use bincode::{deserialize, serialize};
use std::collections::HashMap;
use std::io;
use std::net::Ipv4Addr;
use std::ops::Deref;
use std::str::FromStr;
use std::time::Duration;

use pool::config::{Config, ProducerConfig};
use pool::logger::LOGGER;
use pool::proto::SubmitParams;

use kafka::client::{
    Compression, KafkaClient, RequiredAcks, DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS,
};
use kafka::producer::{AsBytes, Producer, Record, DEFAULT_ACK_TIMEOUT_MILLIS};

const FULLNAME_LIMIT: usize = 38;

#[repr(i32)]
#[derive(Debug)]
pub enum SubmitResult {
    Reject = 0,
    Accept,
}

fn get_inet_addr(worker_addr: &str) -> u32 {
    let addrs = worker_addr
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

fn get_fullname(fullname: &str) -> String {
    let mut fullname = fullname.to_string();
    let length = fullname.len();
    if length < FULLNAME_LIMIT {
        fullname.push_str(" ".repeat(FULLNAME_LIMIT - length).as_str())
    }
    fullname
}

#[repr(C)]
#[derive(Deserialize, Serialize, Clone)]
pub struct Share {
    job_id: u64,
    worker_hash_id: i64, // 0
    difficulty: u64,
    ip: u32,
    user_id: i32, // 0
    timestamp: u32,
    blkbits: u32, // 0
    result: i32,
    height: i32,
    share_diff: u64, // 0
    server_id: u16,
    fullname: String,
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

#[derive(Debug)]
struct ShareWrapper(Vec<u8>);

impl ShareWrapper {
    fn new(share: &Share) -> Self {
        ShareWrapper(serialize(share).unwrap())
    }
}

impl AsBytes for ShareWrapper {
    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

pub struct KafkaProducer {
    pub topic: String,
    pub client: Producer,
    pub partitions: i32,
}

#[derive(Debug, Clone)]
struct KafkaProducerConfig {
    compression: Compression,
    required_acks: RequiredAcks,
    batch_size: usize,
    conn_idle_timeout: Duration,
    ack_timeout: Duration,
}

impl KafkaProducerConfig {
    fn new(
        _compression: Option<&String>,
        _required_acks: Option<&String>,
        _batch_size: Option<&String>,
        _conn_idle_timeout: Option<&String>,
        _ack_timeout: Option<&String>,
    ) -> KafkaProducerConfig {
        KafkaProducerConfig {
            compression: match _compression {
                None => Compression::NONE,
                Some(ref s) if s.eq_ignore_ascii_case("none") => Compression::NONE,
                #[cfg(feature = "gzip")]
                Some(ref s) if s.eq_ignore_ascii_case("gzip") => Compression::GZIP,
                #[cfg(feature = "snappy")]
                Some(ref s) if s.eq_ignore_ascii_case("snappy") => Compression::SNAPPY,
                Some(s) => panic!(format!("Unsupported compression type: {}", s)),
            },
            required_acks: match _required_acks {
                None => RequiredAcks::One,
                Some(ref s) if s.eq_ignore_ascii_case("none") => RequiredAcks::None,
                Some(ref s) if s.eq_ignore_ascii_case("one") => RequiredAcks::One,
                Some(ref s) if s.eq_ignore_ascii_case("all") => RequiredAcks::All,
                Some(s) => panic!(format!("Unknown --required-acks argument: {}", s)),
            },
            batch_size: to_number(_batch_size, 1).unwrap(),
            conn_idle_timeout: Duration::from_millis(
                to_number(_conn_idle_timeout, DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS).unwrap(),
            ),
            ack_timeout: Duration::from_millis(
                to_number(_ack_timeout, DEFAULT_ACK_TIMEOUT_MILLIS).unwrap(),
            ),
        }
    }
}

impl Default for KafkaProducerConfig {
    fn default() -> KafkaProducerConfig {
        KafkaProducerConfig::new(
            None, // Compression NONE
            None, // RequiredAcks One
            None, // batch_size 1
            None, // conn_idle_timeout DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS
            None, // ack_timeout DEFAULT_ACK_TIMEOUT_MILLIS
        )
    }
}

fn to_number<N: FromStr>(s: Option<&String>, _default: N) -> Result<N> {
    match s {
        None => Ok(_default),
        Some(s) => match s.parse::<N>() {
            Ok(n) => Ok(n),
            Err(_) => Ok(_default),
        },
    }
}

pub trait GrinProducer {
    fn from_config(config: &ProducerConfig) -> KafkaProducer;

    fn send_data(&mut self, share: Share) -> Result<()>;
}

impl GrinProducer for KafkaProducer {
    fn from_config(cfg: &ProducerConfig) -> KafkaProducer {
        let mut client = KafkaClient::new(cfg.brokers.clone());
        client.set_client_id("kafka-grin-pool".into());
        match client.load_metadata_all() {
            Ok(_) => {
                let producer = {
                    let options: Option<HashMap<String, String>> = cfg.options.clone();
                    let kafka_config: KafkaProducerConfig;
                    if options.is_some() {
                        let options = options.unwrap();
                        kafka_config = KafkaProducerConfig::new(
                            options.get("compression"),
                            options.get("required_acks"),
                            options.get("batch_size"),
                            options.get("conn_idle_timeout"),
                            options.get("ack_timeout"),
                        );
                    } else {
                        kafka_config = KafkaProducerConfig::default();
                    }
                    Producer::from_client(client)
                        .with_ack_timeout(kafka_config.ack_timeout)
                        .with_required_acks(kafka_config.required_acks)
                        .with_compression(kafka_config.compression)
                        .with_connection_idle_timeout(kafka_config.conn_idle_timeout)
                        .create()
                        .unwrap()
                };

                KafkaProducer {
                    topic: cfg.topic.clone(),
                    partitions: cfg.partitions,
                    client: producer,
                }
            }
            Err(e) => panic!(format!("{:?}", e)),
        }
    }

    fn send_data(&mut self, share: Share) -> Result<()> {
        let record = Record::from_value(&self.topic, ShareWrapper::new(&share));
        self.client.send(&record)?;
        Ok(())
    }
}

error_chain! {
    links {
        Kafka(kafka::error::Error, kafka::error::ErrorKind);
    }
    foreign_links {
        Io(io::Error);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
    use pool::config::{read_config, Config, ProducerConfig};

    #[test]
    fn test_send_data() {
        let config = read_config();
        let mut kafka_producer = KafkaProducer::from_config(&config.producer);
        let share = Share::new(
            "test_server_id".to_owned(),
            2019usize,
            "test_server_address".to_owned(),
            9981u64,
            "test_worker_fullname".to_owned(),
            SubmitResult::Accept,
            10u64,
            4u64,
        );
        let result = kafka_producer.send_data(share);
        assert_eq!(result.is_ok(), true, "{}", format!("{:?}", result));
    }

    #[test]
    fn test_consumer_data_from_kafka() {
        let config = read_config();
        let mut kafka_producer = KafkaProducer::from_config(&config.producer);
        let share = Share::new(
            "test_server_id".to_owned(),
            2019usize,
            "test_server_address".to_owned(),
            9981u64,
            "test_worker_fullname".to_owned(),
            SubmitResult::Accept,
            10u64,
            4u64,
        );
        struct Inner {
            pub producer: KafkaProducer,
        }

        let mut inner = Inner {
            producer: kafka_producer,
        };
        let result = inner.producer.send_data(share.clone());
        assert_eq!(result.is_ok(), true, "{}", format!("{:?}", result));

        let cfg: &ProducerConfig = &config.producer;
        let mut consumer = {
            let mut cb = Consumer::from_hosts(cfg.brokers.clone())
                .with_group(String::new())
                .with_fallback_offset(FetchOffset::Earliest)
                .with_fetch_max_wait_time(Duration::from_millis(2))
                .with_fetch_min_bytes(1_000)
                .with_fetch_max_bytes_per_partition(100_100)
                .with_retry_max_bytes_limit(1_000_000)
                .with_offset_storage(GroupOffsetStorage::Kafka)
                .with_client_id("kafka-grin-test-consumer".into());
            cb = cb.with_topic(cfg.topic.clone());
            cb.create().unwrap()
        };

        let mut messages = consumer.poll().unwrap();
        let mut messages_iter = messages.iter();
        let message_set = messages_iter.next().unwrap();

        let message_content: &[u8] = message_set.messages()[message_set.messages().len() - 1].value;
        let s: Share = deserialize(message_content).unwrap();
        assert_eq!(s.accepted, share.accepted);
        assert_eq!(s.rejected, share.rejected);
        assert_eq!(s.difficulty, share.difficulty);
        assert_eq!(s.worker_id, share.worker_id);
        assert_eq!(s.fullname, share.fullname);
        assert_eq!(s.server_id, share.server_id);
        assert_eq!(s.worker_addr, share.worker_addr);
    }
}
