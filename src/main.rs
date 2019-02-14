extern crate influent;
extern crate rand;
extern crate sha2;
extern crate time;
extern crate tokio_core;

use influent::create_client;
use influent::client::{Client, Credentials};
use influent::measurement::{Measurement, Value};
use rand::Rng;
use sha2::{Sha256, Digest};
use tokio_core::reactor::Core;


static MEASUREMENT: &str = "table";
static TAG: &str = "hash";
static FIELD_ONE: &str = "from";
static FIELD_TWO: &str = "to";
static FIELD_THREE: &str = "balance";

static SERIES: usize = 10;
static POINTS_PER_SERIES: usize = 10000;
static QUERIES: usize = 50000;

struct Executor<'a> {
    client: &'a Client,
    reactor: Core,
}

impl<'a> Executor<'a> {
    
    fn new(client: &'a Client, reactor: Core) -> Executor<'a> {
        return Executor {
            client: client,
            reactor: reactor
        }
    }
    
    pub fn get_client(&self) -> &Client {
        return self.client
    }
    
    pub fn get_reactor(&mut self) -> &mut Core {
        return &mut self.reactor
    }
}

fn main() {
    let credentials = Credentials {
        username: "root",
        password: "root",
        database: "many_points"
    };
    let hosts = vec!["http://localhost:8086"];
    let client = create_client(credentials, hosts);
    let reactor = Core::new().unwrap();
    
    let mut executor = Executor::new(&client, reactor);
    
    let tags = insert_points(&mut executor, SERIES, POINTS_PER_SERIES);
    start_benchmark(&mut executor, tags, QUERIES, POINTS_PER_SERIES);
}

fn insert_points(executor: &mut Executor, series: usize, points_per_series: usize) -> Vec<String> {
    let mut tags = Vec::new();
    if series == 0 {
        return tags
    } 
    
    let mut rng = rand::thread_rng();
    
    for _ in 0..series {
        let random_number = rng.gen::<usize>();
        let hash = generate_hash_from_number(&random_number);
        tags.push(hash.clone());
        
        insert_one_point(executor, hash, random_number)
    }
    
    if points_per_series == 0 {
        return tags
    } 
    
    let points_count = series*(points_per_series-1);
    let max_tag_pos = series-1;
    
    for _ in 0..points_count {
        let random_number = if max_tag_pos > 0 { rng.gen_range(0, max_tag_pos) } else { 0 };
        let hash = tags.get(random_number).unwrap();
        
        insert_one_point(executor, hash.to_string(), random_number)
    }  
    
    return tags
}

fn start_benchmark(executor: &mut Executor, tags: Vec<String>, queries: usize, points_per_series: usize) {
    if tags.len() == 0 {
        return
    } 
    
    let mut rng = rand::thread_rng();
    let max_tag_pos = tags.len()-1;
    let mut queries_time_ms: i32 = 0;
    
    for i in 0..queries {
        let random_number = if max_tag_pos > 0 { rng.gen_range(0, max_tag_pos) } else { 0 };
        let hash = tags.get(random_number).unwrap();
        
        let query = format!("select * from {} where {} = '{}'", MEASUREMENT, TAG, hash);
        let res = executor.get_client().query(query, None);
        
        let start_time = time::now();
        executor.get_reactor().run(res);
        let end_time = time::now();
        
        let query_time = compute_time_diff_ms(start_time, end_time);
        queries_time_ms += query_time;
        let avverag_time: i32 = queries_time_ms/(i+1) as i32;
        println!("Query №{} avverage time: {} ms, query time: {} ms", i, avverag_time, query_time);
    }
    println!("{} queries for {} ms, {} entities per query", queries, queries_time_ms, points_per_series);
}

fn generate_hash_from_number(id: &usize) -> String {
    let mut hasher = Sha256::new();
    hasher.input(id.to_string().into_bytes());
    return format!("{:x}", hasher.result())
}

fn insert_one_point(executor: &mut Executor, hash: String, random_number: usize) {
    let mut from = hash.clone();
    let mut to = hash.clone();
    from.push_str("from");
    to.push_str("to");
    
    let mut measurement = Measurement::new(MEASUREMENT);
    measurement.add_tag(TAG, hash);
    measurement.add_field(FIELD_ONE, Value::String(&from));
    measurement.add_field(FIELD_TWO, Value::String(&to));
    measurement.add_field(FIELD_THREE, Value::Integer(random_number as i64));
    
    let res = executor.get_client().write_one(measurement, None);
    executor.get_reactor().run(res);
}

fn compute_time_diff_ms(start_time: time::Tm, end_time: time::Tm) -> i32 {
    return (end_time.tm_nsec-start_time.tm_nsec)/1000000
}

