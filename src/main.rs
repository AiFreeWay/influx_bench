mod influxdb_bench;
mod mongodb_bench;


extern crate influent;
#[macro_use(bson, doc)]
extern crate mongodb;
extern crate rand;
extern crate serde_json;
extern crate sha2;
extern crate tokio_core;

use influxdb_bench::ExecutorInflux;
use mongodb_bench::ExecutorMongo;
use sha2::{Sha256, Digest};
use serde_json::Value;
use std::fmt::Arguments;
use std::fs::File;
use std::io::{Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};
use rand::Rng;


static CONFIG_FILE_PATH: &str = "config.json";
static INFLUX_TEST_CASES_FILE_PATH: &str = "influx_testcases.json";
static MONGO_TEST_CASES_FILE_PATH: &str = "mongo_testcases.json";

pub trait Executor {
    fn insert(&mut self, hash: String, random_number: usize) -> i32;
    fn select(&mut self, hash: &String) -> i32;
}

pub struct TestCase {
    id: usize,
    series: usize,
    points_per_series: usize,
    queries: usize
}

impl TestCase {
    
    pub fn new(id: usize,series: usize, points_per_series: usize, queries: usize) -> TestCase {

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

pub struct TestEnviroment<'a> {
    executor: &'a mut Executor,
    log_file: File,
}

enum DatabaseType {
    Influxdb = 1,
    Mongodb = 2,
}

fn main() {
    match get_database_type() {
        DatabaseType::Influxdb => {
            let executor = &mut ExecutorInflux::new();
            let test_cases = get_test_cases(INFLUX_TEST_CASES_FILE_PATH);
            println!("# Start InfluxDB benchmark");
            start_benchmark(String::from("InfluxDB"), executor, test_cases);
        },
        DatabaseType::Mongodb => {
            let executor = &mut ExecutorMongo::new();
            let test_cases = get_test_cases(MONGO_TEST_CASES_FILE_PATH);
            println!("# Start MongoDB benchmark");
            start_benchmark(String::from("MongoDB"), executor, test_cases);
        },
    }
}


pub fn start_benchmark(db_name: String, executor: &mut Executor, test_cases: Vec<TestCase>) {
    for test_case in test_cases {
        println!("Begin test case: {}", test_case.get_id());
        let mut log_file = File::create(format!("{}_log{}.txt", db_name, test_case.get_id()))
            .expect("Can't create log file");
        let mut test_env = TestEnviroment { executor, log_file };
        
        let tags = insert_points(&mut test_env, test_case.get_series(), test_case.get_points_per_series());
        start_testcase(&mut test_env, tags.clone(), test_case.get_queries(), test_case.get_points_per_series());
    }
}

fn insert_points(test_env: &mut TestEnviroment,
    series: usize, 
    points_per_series: usize) -> Vec<String> {
        
    if series == 0 {
        return Vec::new();
    }
    write_log(&mut test_env.log_file, format_args!("# Begin insert series\n"));   
    
    let mut rng = rand::thread_rng();
    let data_getter = || {
        let random_number = rng.gen::<usize>();
        return (generate_hash_from_number(&random_number), random_number);
    };
    
    let tags = insert_records(test_env, true, series, data_getter);
    
    if points_per_series == 0 {
        return tags
    } 
    write_log(&mut test_env.log_file, format_args!("# Begin insert points\n"));   
    
    let points_count = series*(points_per_series-1);
    let mut max_tag_pos = series;
    let mut rng = rand::thread_rng();
    let mut tags_series = tags.clone();
    let mut points: Vec<usize> = vec![0; series];
    let data_getter = || {
        let random_number = if max_tag_pos-1 > 0 { rng.gen_range(0, max_tag_pos-1) } else { 0 };
        points[random_number] += 1;
        let hash = tags_series.get(random_number).unwrap().to_string();
        points[random_number] += 1;
        if points[random_number] == points_per_series {
            points.remove(random_number);
            tags_series.remove(random_number);
            max_tag_pos -= 1;
        }
        
        return (hash, random_number);
    };
    insert_records(test_env, false, points_count, data_getter);
    return tags;
}

fn insert_records<F>(test_env: &mut TestEnviroment, 
    is_need_collect_tags: bool,
    iteration_count: usize, 
    mut data_getter: F) -> Vec<String> 
    where F : FnMut() -> (String, usize)  {
    
    let mut tags = Vec::new();
    let mut queries_time_ms: i32 = 0;
    let log_frequency = iteration_count/500;
    let mut iteration_between_log = 0;
        
    for i in 0..iteration_count {
        iteration_between_log += 1;
        let (hash, random_number) = data_getter();
        
        if is_need_collect_tags {
            tags.push(hash.clone());
        }
        
        let query_time = test_env.executor.insert(hash, random_number);
        queries_time_ms += query_time;
        
        if iteration_between_log == log_frequency {
            iteration_between_log = 0;
            let average_time: i32 = queries_time_ms/(i+1) as i32;
            
            write_log(&mut test_env.log_file, 
                format_args!("Insert query; average time: {} ms, query time: {} ms\n", 
                average_time, 
                query_time));
        }
    }
    
    write_log(&mut test_env.log_file,
        format_args!("# Insert {} queries for {} ms\n", 
        iteration_count, 
        queries_time_ms));
    
    return tags
}

fn start_testcase(test_env: &mut TestEnviroment, 
    tags: Vec<String>, 
    queries: usize, 
    points_per_series: usize) {
        
    write_log(&mut test_env.log_file, format_args!("# Begin select queries\n"));  
          
    if tags.len() == 0 {
        return
    } 
    
    let mut rng = rand::thread_rng();
    let max_tag_pos = tags.len()-1;
    let mut queries_time_ms: i32 = 0;
    
    for i in 0..queries {
        let random_number = if max_tag_pos > 0 { rng.gen_range(0, max_tag_pos) } else { 0 };
        let hash = tags.get(random_number).unwrap();
        
        let query_time = test_env.executor.select(hash);
        
        queries_time_ms += query_time;
        let average_time: i32 = queries_time_ms/(i+1) as i32;
        write_log(&mut test_env.log_file, 
            format_args!("Select query №{} average time: {} ms, query time: {} ms\n", 
            i, 
            average_time, 
            query_time));
    }
    let points_per_series = if points_per_series > 0 { points_per_series } else { 1 };
    write_log(&mut test_env.log_file,
        format_args!("# Select {} queries for {} ms, {} entities per query\n", 
        queries, 
        queries_time_ms, 
        points_per_series));
}

pub fn generate_hash_from_number(id: &usize) -> String {
    let mut hasher = Sha256::new();
    hasher.input(id.to_string().into_bytes());
    return format!("{:x}", hasher.result())
}

pub fn get_current_time() -> u128 {
    let system_time = SystemTime::now().duration_since(UNIX_EPOCH)
        .expect("Time went backwards"); 
    return system_time.as_millis();    
}

pub fn compute_time_diff_ms(start_time: u128, end_time: u128) -> i32 {
    return (end_time-start_time) as i32;
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
    let mut file = File::open(test_case_file_path).expect("Can't open tests file");
    let mut contents = String::new();
    file.read_to_string(&mut contents).expect("Can't read tests file");
    
    let json: Value = serde_json::from_str(&contents).expect("Invalid tests file json format");
    let mut test_cases = Vec::new();
    
    for test_case_json in json["test_cases"].as_array().expect("Invalid test_cases array param") {
        let id = test_case_json["id"].as_u64().expect("Invalid test_case id param") as usize;
        let series = test_case_json["series"].as_u64().expect("Invalid test_case id param") as usize;
        let points_per_series = test_case_json["points_per_series"].as_u64().expect("Invalid test_case id param") as usize;
        let queries = test_case_json["queries"].as_u64().expect("Invalid test_case id param") as usize;
        
        let test_case = TestCase::new(id, series, points_per_series, queries);
        test_cases.push(test_case);
    }
    
    return test_cases;
}

fn write_log(log_file: &mut File, args: Arguments) {
    log_file.write_fmt(args);
}
