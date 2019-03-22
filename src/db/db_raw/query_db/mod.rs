mod rethink_facade;

use self::rethink_facade::Database;
use db::reql::errors::Error;
use db::serde_json;
use db::serde_json::Value;
use std::collections::HashMap;


pub type Args = HashMap<String, String>;
pub type DBError = Error;
pub type Table = String;
pub type Response = Value;

pub fn get_database() -> Result<Box<QueryBase>, DBError> {
    return match Database::new() {
        Ok(db) => Ok(Box::new(db)),
        Err(err) => Err(err)
    }
}

pub struct Request {
    table: Table,
    data: Option<Args>,
    condition: Option<Args>,
}

impl Request {
    pub fn new(table: Table, data: Args, condition: Args) -> Request {
        return Request {
            table: table,
            data: Some(data),
            condition: Some(condition)
        }
    }
    
    pub fn from_condition(table: Table, condition: Args) -> Request {
        return Request {
            table: table,
            data: None,
            condition: Some(condition)
        }
    }
    
    pub fn from_data(table: Table, data: Args) -> Request {
        return Request {
            table: table,
            data: Some(data),
            condition: None
        }
    }
    
    pub fn data_to_json(&self) -> Value {
        return Request::args_to_json(&self.data.clone());
    }
    
    pub fn condition_to_json(&self) -> Value {
        return Request::args_to_json(&self.condition.clone());
    }
    
    fn args_to_json(args_opt: &Option<Args>) -> Value {
        let args_clone = args_opt.clone();
        let args = args_clone.unwrap_or(HashMap::new());
        return serde_json::to_value(args).unwrap();
    }
}

pub trait QueryBase: Sync {
    fn get_indexes(&self, table: &str) -> Result<Value, DBError>;
    fn create_index(&self, table: &str, index: &str) -> Option<DBError>;
    fn insert(&self, request: Request) -> Option<DBError>;
    fn update(&self, request: Request) -> Option<DBError>;
    fn select(&self, request: Request) -> Result<Response, DBError>;
    fn delete(&self, request: Request) -> Option<DBError>;
}