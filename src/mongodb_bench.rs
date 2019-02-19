use {compute_time_diff_ms, generate_hash_from_number, TestCase};
use mongodb::{Client, ThreadedClient};
use mongodb::coll::Collection;
use mongodb::coll::options::IndexOptions;
use mongodb::db::ThreadedDatabase;
use rand::Rng;


static COLLECTION: &str = "accounts";
static TAG: &str = "address";
static FIELD_ONE: &str = "from";
static FIELD_TWO: &str = "to";
static FIELD_THREE: &str = "balance";

pub fn start_benchmark(test_cases: Vec<TestCase>) {
    println!("Begin MognoDB benchmark");
    let client = Client::connect("localhost", 27017)
        .ok()
        .expect("Failed to initialize client.");
    let collection = client.db("bench").collection(COLLECTION);    
        
    for test_case in test_cases {
        println!("# Begin test case: {}", test_case.get_id());
        let tags = insert_records(&collection, test_case.get_series(), test_case.get_points_per_series());
        start_testcase(&collection, tags.clone(), test_case.get_queries(), test_case.get_points_per_series());
    }
}

fn insert_records(collection: &Collection, series: usize, points_per_series: usize) -> Vec<String> {
    let mut tags = Vec::new();
    if series == 0 {
        return tags
    } 
    
    let mut rng = rand::thread_rng();
    
    for _ in 0..series {
        let random_number = rng.gen::<usize>();
        let hash = generate_hash_from_number(&random_number);
        tags.push(hash.clone());
        
        insert_one_record(&collection, hash, random_number)
    }
    
    if points_per_series == 0 {
        return tags
    } 
    
    let points_count = series*(points_per_series-1);
    let max_tag_pos = series-1;
    
    for _ in 0..points_count {
        let random_number = rng.gen::<usize>();
        let random_pos = if max_tag_pos > 0 { rng.gen_range(0, max_tag_pos) } else { 0 };
        let hash = tags.get(random_pos).unwrap();    
        
        insert_one_record(&collection, hash.to_string(), random_number)
    }  
    
    return tags
}

fn insert_one_record(collection: &Collection, hash: String, random_number: usize) {
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
    
    collection.insert_one(doc.clone(), None).ok().expect("Failed to insert document.");
    let mut index = IndexOptions::new();
    index.name = Some(hash);
    collection.create_index(doc, Some(index));
}

fn start_testcase(collection: &Collection, tags: Vec<String>, queries: usize, points_per_series: usize) {
    if tags.len() == 0 {
        return
    } 
    
    let mut rng = rand::thread_rng();
    let max_tag_pos = tags.len()-1;
    let mut queries_time_ms: i32 = 0;
    
    for i in 0..queries {
        let random_number = if max_tag_pos > 0 { rng.gen_range(0, max_tag_pos) } else { 0 };
        let hash = tags.get(random_number).unwrap();
        
        let doc = doc!{
            TAG: hash
        };
        
        let start_time = time::now();
        collection.find(Some(doc), None).ok().expect("Failed to execute find.");
        let end_time = time::now();
        
        let query_time = compute_time_diff_ms(start_time, end_time);
        queries_time_ms += query_time;
        let average_time: i32 = queries_time_ms/(i+1) as i32;
        println!("Query №{} average time: {} ms, query time: {} ms", i, average_time, query_time);
    }
    let points_per_series = if points_per_series > 0 { points_per_series } else { 1 };
    println!("{} queries for {} ms, {} entities per query", queries, queries_time_ms, points_per_series);
}