use {compute_time_diff_ms, Executor, get_current_time};
use db::db_raw::query_db;
use db::db_raw::query_db::{Request, QueryBase};
use std::collections::HashMap;


static TABLE: &str = "test";
static TAG: &str = "address";
static FIELD_ONE: &str = "from";
static FIELD_TWO: &str = "to";
static FIELD_THREE: &str = "balance";

pub struct ExecutorRethink {
    database: Box<QueryBase>
}

impl ExecutorRethink {
    pub fn new() -> ExecutorRethink {
        return ExecutorRethink {
                database: query_db::get_database().unwrap(),
        }
    }
}

impl Executor for ExecutorRethink {
    fn insert(&mut self, hash: String, random_number: usize) -> i32 {
        let mut from = hash.clone();
        let mut to = hash.clone();
        from.push_str("from");
        to.push_str("to");
        
        let mut data = HashMap::new();
        data.insert(String::from(TAG), hash.clone());
        data.insert(String::from(FIELD_ONE), from);
        data.insert(String::from(FIELD_TWO), to);
        data.insert(String::from(FIELD_THREE), random_number.to_string());
        let request_insert = Request::from_data(String::from(TABLE), data);
        
        let start_time = get_current_time();
        self.database.insert(request_insert);
        let end_time = get_current_time();
        
        return compute_time_diff_ms(start_time, end_time);
    }
    
    fn select(&mut self, hash: &String) -> i32 {
        let mut data = HashMap::new();
        data.insert(String::from(TAG), hash.clone());
        let request_select = Request::from_condition(String::from(TABLE), data);
        
        let start_time = get_current_time();
        self.database.select(request_select).expect("Failed to execute select");
        
        let end_time = get_current_time();
        
        return compute_time_diff_ms(start_time, end_time);
    }
}