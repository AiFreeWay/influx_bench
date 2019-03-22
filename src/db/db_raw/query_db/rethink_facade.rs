use super::*;
use db::futures::stream::Stream;
use db::reql::{Config, Connection, Client, Document, IntoArg, Run};
use db::reql::errors::{Error, DriverError};
use std::sync::Arc;


pub type Response = Option<Result<Option<Document<Value>>, Error>>;

pub struct Database {
    client: Client,
    connection: Connection
}

impl Database {
    pub fn new() -> Result<Database, DBError> {
        let client = Client::new();
        
        return client.connect(Config::default()).map(|connection| {
            Database {
                client: client,
                connection: connection
            }
        })
    }
    
    fn map_response_to_json(response: Response) -> Result<Value, DBError> {
        let err = Error::Driver(Arc::new(DriverError::Other("Empty response".to_string())));
        return response.ok_or(err.clone())
            .and_then(|response_res| response_res)
            .map(|document_opt| document_opt.ok_or(err))
            .and_then(|document_res| document_res)
            .map(|document| {
                return match document {
                    Document::Expected(value) => value,
                    Document::Unexpected(value) => value
                }
            });
    }
}

impl QueryBase for Database {
    fn get_indexes(&self, table: &str) -> Result<Value, DBError> {
        return self.client
            .table(table)
            .index_list()
            .run::<Value>(self.connection)
            .map(|request| request.wait().next())
            .and_then(|response| Database::map_response_to_json(response));
    }
    
    fn create_index(&self, table: &str, index: &str) -> Option<DBError> {
        //r.table('comments').indexCreate('postId').run(conn, callback)
        None
    }
    
    fn insert(&self, request: Request) -> Option<DBError> {
        return self.client
            .table(&request.table)
            .insert(request.data_to_json())
            .run::<Value>(self.connection)
            .map(|request| request.wait().next())
            .err();
    }
    
    fn update(&self, request: Request) -> Option<DBError> {
        return self.client
            .table(&request.table)
            .filter(request.condition_to_json())
            .update(request.data_to_json())
            .run::<Value>(self.connection)
            .map(|request| request.wait().next())
            .err();
    }
    
    fn select(&self, request: Request) -> Result<Value, DBError> {
        return self.client
            .table(&request.table)
            .filter(request.condition_to_json())
            .run::<Value>(self.connection)
            .map(|request| request.wait().next())
            .and_then(|response| Database::map_response_to_json(response));
    }
    
    fn delete(&self, request: Request) -> Option<DBError> {
        return self.client
            .table(&request.table)
            .delete()
            .with_args(request.condition_to_json())
            .run::<Value>(self.connection)
            .map(|request| request.wait().next())
            .err();
    }
}