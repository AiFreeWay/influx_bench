use {compute_time_diff_ms, Executor, get_current_time};
use mongodb::{Client, ThreadedClient};
use mongodb::coll::Collection;
use mongodb::db::ThreadedDatabase;


static COLLECTION: &str = "accounts";
static TAG: &str = "address";
static FIELD_ONE: &str = "from";
static FIELD_TWO: &str = "to";
static FIELD_THREE: &str = "balance";

pub struct ExecutorMongo {
    collection: Collection,
    indexed: bool
}

impl ExecutorMongo {
    pub fn new() -> ExecutorMongo {
        let client = Client::connect("localhost", 27017)
            .ok()
            .expect("Failed to initialize client");
            
        let database = client.db("bench");
        database.auth("admin", "password").expect("Excect auth");
        let collection = database.collection(COLLECTION); 
        
        return ExecutorMongo {
            collection: collection,
            indexed: false
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
        
        let start_time = get_current_time();
        self.collection.insert_one(doc.clone(), None).ok().expect("Failed to execute insert");
        let end_time = get_current_time();
        
        if !self.indexed {
            let doc = doc! { TAG: 1 };
            self.collection.create_index(doc, None);
            self.indexed = true;
        }
        
        return compute_time_diff_ms(start_time, end_time);
    }
    
    fn select(&mut self, hash: &String) -> i32 {
        let doc = doc!{
            TAG: hash
        };
        
        let start_time = get_current_time();
        self.collection.find(Some(doc), None).ok().expect("Failed to execute find");
        let end_time = get_current_time();
        
        return compute_time_diff_ms(start_time, end_time);
    }
}