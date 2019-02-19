mod influxdb_bench;
mod mongodb_bench;


extern crate influent;
#[macro_use(bson, doc)]
extern crate mongodb;
extern crate rand;
extern crate serde_json;
extern crate sha2;
extern crate time;
extern crate tokio_core;

use sha2::{Sha256, Digest};
use serde_json::Value;
use std::fs::File;
use std::io::Read;


static CONFIG_FILE_PATH: &str = "config.json";
static INFLUX_TEST_CASES_FILE_PATH: &str = "influx_testcases.json";
static MONGO_TEST_CASES_FILE_PATH: &str = "mongo_testcases.json";

pub struct TestCase {
    id: usize,
    series: usize,
    points_per_series: usize,
    queries: usize
}

impl TestCase {
    
    pub fn new(id: usize, series: usize, points_per_series: usize, queries: usize) -> TestCase {
        return TestCase {
            id: id,
            series: series,
            points_per_series: points_per_series,
            queries: queries
        }
    }
    
    pub fn get_id(&self) -> usize {
        return self.id
    }
    
    pub fn get_series(&self) -> usize {
        return self.series
    }
    
    pub fn get_points_per_series(&self) -> usize {
        return self.points_per_series
    }
    
    pub fn get_queries(&self) -> usize {
        return self.queries
    }
}

enum DatabaseType {
    Influxdb = 1,
    Mongodb = 2,
}

fn main() {
    match get_database_type() {
        DatabaseType::Influxdb =>  {
            let test_cases = get_test_cases(INFLUX_TEST_CASES_FILE_PATH);
            influxdb_bench::start_benchmark(test_cases)
        },
        DatabaseType::Mongodb =>  {
            let test_cases = get_test_cases(MONGO_TEST_CASES_FILE_PATH);
            mongodb_bench::start_benchmark(test_cases)
        },
    }
}

pub fn compute_time_diff_ms(start_time: time::Tm, end_time: time::Tm) -> i32 {
    let start_ns = start_time.tm_nsec;
    let end_ns = end_time.tm_nsec;
    if start_ns > end_ns {
        return (start_ns-end_ns)/1000000
    }
    return (end_ns-start_ns)/1000000
}

pub fn generate_hash_from_number(id: &usize) -> String {
    let mut hasher = Sha256::new();
    hasher.input(id.to_string().into_bytes());
    return format!("{:x}", hasher.result())
}

fn get_database_type() -> DatabaseType {
    let mut file = File::open(CONFIG_FILE_PATH).expect("Can't open configuration file");
    let mut contents = String::new();
    file.read_to_string(&mut contents).expect("Can't read configuration file");
    
    let json: Value = serde_json::from_str(&contents).expect("Invalid json format");
    return match json["database_type"].as_u64().expect("Invalid database_type params") {
        1 => DatabaseType::Influxdb,
        2 => DatabaseType::Mongodb,
        _ => panic!("Invalid database_type variant")
    }
}

fn get_test_cases(test_case_file_path: &str) -> Vec<TestCase> {
    let mut file = File::open(test_case_file_path).expect("Can't open influx tests file");
    let mut contents = String::new();
    file.read_to_string(&mut contents).expect("Can't read influx tests file");
    
    let json: Value = serde_json::from_str(&contents).expect("Invalid influx tests file json format");
    let mut test_cases = Vec::new();
    
    for test_case_json in json["test_cases"].as_array().expect("Invalid influx test_cases array param") {
        let id = test_case_json["id"].as_u64().expect("Invalid influx test_case id param") as usize;
        let series = test_case_json["series"].as_u64().expect("Invalid influx test_case id param") as usize;
        let points_per_series = test_case_json["points_per_series"].as_u64().expect("Invalid influx test_case id param") as usize;
        let queries = test_case_json["queries"].as_u64().expect("Invalid influx test_case id param") as usize;
        
        let test_case = TestCase::new(id, series, points_per_series, queries);
        test_cases.push(test_case);
    }
    
    return test_cases;
}

