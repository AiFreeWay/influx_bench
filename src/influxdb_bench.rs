use {compute_time_diff_ms, Executor, get_current_time, parse_query};
use influent::create_client;
use influent::client::{Client, Credentials};
use influent::client::http::HttpClient;
use influent::measurement::{Measurement, Value};
use tokio_core::reactor::Core;
use rand::Rng;


static MEASUREMENT: &str = "accounts";
static TAG: &str = "address";
static FIELD_ONE: &str = "from";
static FIELD_TWO: &str = "to";
static FIELD_THREE: &str = "balance";

pub struct ExecutorInflux<'a> {
    reactor: Core,
    client: HttpClient<'a>
}

impl<'a> ExecutorInflux<'a> {
    pub fn new() -> ExecutorInflux<'a> {
        let credentials = Credentials {
            username: "root",
            password: "root",
            database: "bench"
        };
        
        let reactor = Core::new().unwrap();
        let hosts = vec!["http://localhost:8086"];
        let client = create_client(credentials, hosts);
        
        return ExecutorInflux {
            reactor: reactor,
            client: client
        }
    }
    
    pub fn get_hashes(&mut self) -> Vec<String> {
        let mut rng = rand::thread_rng();
        let mut tags = Vec::new();
        let mut offset = 0;
        let hashes = 1000;
        let mut i = 0;
        
        while tags.len() < hashes {
            let random_number = rng.gen_range(1, 10000);
            if offset > 50000000 {
                offset = 0;
            }
            offset = offset + random_number;
            let query = format!("select \"from\" from {} limit 1 offset {}", MEASUREMENT, offset);
            let request = self.client.query(query, None);
            let result = self.reactor.run(request);
            
            match result {
                Ok(value) => {
                    let result = parse_query(value);
                    if !tags.contains(&result) {
                        i = i+1;
                        tags.push(result);
                        println!("{} from {}", i, hashes);
                    }
                }, 
                _ => {},
            };
        }
        return tags
    }
}

impl<'a> Executor for ExecutorInflux<'a> {
    fn insert(&mut self, hash: String, random_number: usize) -> i32 {
        let mut from = hash.clone();
        let mut to = hash.clone();
        from.push_str("from");
        to.push_str("to");
        
        let mut measurement = Measurement::new(MEASUREMENT);
        measurement.add_tag(TAG, hash);
        measurement.add_field(FIELD_ONE, Value::String(&from));
        measurement.add_field(FIELD_TWO, Value::String(&to));
        measurement.add_field(FIELD_THREE, Value::Integer(random_number as i64));
        
        let res = self.client.write_one(measurement, None);
        
        let start_time = get_current_time();
        self.reactor.run(res);
        let end_time = get_current_time();
        
        return compute_time_diff_ms(start_time, end_time);
    }
    
    fn select(&mut self, hash: &String) -> i32 {
        let query = format!("select * from {} where {} = '{}'", MEASUREMENT, TAG, hash);
        let res = self.client.query(query, None);
        
        let start_time = get_current_time();
        self.reactor.run(res);
        let end_time = get_current_time();
        
        return compute_time_diff_ms(start_time, end_time);
    }
}