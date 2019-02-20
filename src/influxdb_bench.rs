use {compute_time_diff_ms, Executor, get_current_time};
use influent::create_client;
use influent::client::{Client, Credentials};
use influent::client::http::HttpClient;
use influent::measurement::{Measurement, Value};
use tokio_core::reactor::Core;


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