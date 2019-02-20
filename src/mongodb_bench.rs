use {compute_time_diff_ms, Executor, get_current_time};
use mongodb::{Client, ThreadedClient};
use mongodb::coll::Collection;
use mongodb::coll::options::IndexOptions;
use mongodb::db::ThreadedDatabase;


static COLLECTION: &str = "accounts";
static TAG: &str = "address";
static FIELD_ONE: &str = "from";
static FIELD_TWO: &str = "to";
static FIELD_THREE: &str = "balance";

pub struct ExecutorMongo {
    collection: Collection
}

impl ExecutorMongo {
    pub fn new() -> ExecutorMongo {
        let client = Client::connect("localhost", 27017)
            .ok()
            .expect("Failed to initialize client.");
        let collection = client.db("bench").collection(COLLECTION);  
        
        return ExecutorMongo {
            collection: collection
        }
    }
}

impl Executor for ExecutorMongo {
    fn insert(&mut self, hash: String, random_number: usize) -> i32 {
        let mut from = hash.clone();
        let mut to = hash.clone();
        from.push_str("from");
        to.push_str("to");
        
        let doc = doc! {
            TAG: hash.clone(),
            FIELD_ONE: from,
            FIELD_TWO: to,
            FIELD_THREE: random_number as u64
        };
        
        let mut index = IndexOptions::new();
        index.name = Some(hash);
        
        let start_time = get_current_time();
        self.collection.insert_one(doc.clone(), None).ok().expect("Failed to insert document.");
        self.collection.create_index(doc, Some(index));
        let end_time = get_current_time();
        
        return compute_time_diff_ms(start_time, end_time);
    }
    
    fn select(&mut self, hash: &String) -> i32 {
        let doc = doc!{
            TAG: hash
        };
        
        let start_time = get_current_time();
        self.collection.find(Some(doc), None).ok().expect("Failed to execute find.");
        let end_time = get_current_time();
        
        return compute_time_diff_ms(start_time, end_time);
    }
}