use std::path::Path;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;
use rocksdb::{DB, Options, DBCompactionStyle, Error, IteratorMode};
use warp::{Filter, Reply, reply};
use serde_json::{Value, json};
use warp::http::{StatusCode, Response};
use tokio;

struct Server {
    docs: DB,
    port: String,
}

impl Server {
    pub fn new(db_name: &str, port: &str) -> Result<Self, Error> {
        let db_path = Path::new(db_name);
        let docs = DB::open_default(db_path)?;

        Ok(Self {
            docs: docs,
            port: port.to_string() 
        })
    }
    async fn reindex(&self) {
        // Reindexing logic goes here
        panic!("Not implemented")
    }

    async fn add_document(self: Arc<Self>, document: Value) -> Result<impl warp::Reply, warp::Rejection> {
        let id = Uuid::new_v4().to_string();
        let doc = serde_json::to_string(&document).unwrap();
        // write to db
        let write_options = rocksdb::WriteOptions::default();
        self.docs.put_opt(id.clone(), doc, &write_options).unwrap();
        // response
        let status = StatusCode::CREATED;
        let response = reply::json(&json!({ "id": id, "status": status.as_str()}));
        Ok(reply::with_status(response, status))
    }

    async fn get_document(self: Arc<Self>, id: String) -> Result<impl warp::Reply, warp::Rejection> {
        // read from db
        let doc = self.get_document_by_id(id).unwrap();
        let doc = json!(doc);
        // response
        let status = if doc.is_null() {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::OK
        };
        let body = json!({ "status": status.as_str(), "doc": doc });
        let response = reply::json(&body);
        Ok(reply::with_status(response, status))
    }
    // helper
    fn get_document_by_id(&self, id: String) -> Result<HashMap<String, String>, Error> {
        let read_options = rocksdb::ReadOptions::default();
        let doc = self.docs.get_opt(id, &read_options).unwrap();
        // make it to string
        let doc = String::from_utf8(doc.unwrap()).unwrap();
        // convert to json
        let doc: HashMap<String, String> = serde_json::from_str(&doc).unwrap();
        Ok(doc)
    }

    async fn search_documents(self: Arc<Self>, q: &String) -> Result<impl warp::Reply, warp::Rejection> {
        let query = match parse_query(q) {
            Ok(q) => q,
            Err(e) => {
                return Ok(warp::reply::with_status(
                    format!("Invalid query: {}", e),
                    StatusCode::BAD_REQUEST
                ))
            }
        };

        let mut documents = Vec::new();

        for entry in self.docs.iterator(IteratorMode::Start) {
            match entry {
                Ok((key, value)) => {
                    let document = match serde_json::from_slice(&value) {
                        Ok(doc) => doc,
                        Err(e) => {
                            return Ok(warp::reply::with_status(
                                format!("Error deserializing document: {:?}", e),
                                StatusCode::INTERNAL_SERVER_ERROR
                            ))
                        }
                    };

                    if query.matches(&document) {
                        documents.push(json!({
                            "id": String::from_utf8(key.to_vec()).unwrap(),
                            "body": document,
                        }));
                    }
                }
                Err(e) => {
                    return Ok(warp::reply::with_status(
                        format!("Database error: {:?}", e),
                        StatusCode::INTERNAL_SERVER_ERROR
                    ))
                }
            }
        }

        let response = json!({
            "documents": documents,
            "count": documents.len(),
        });
        Ok(warp::reply::with_status(
            response.to_string(),
            StatusCode::OK
        ))
    }
}


fn get_path(doc: Value, parts: &[String]) -> Value {
    let mut current = &doc;

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let value = current.get(part);
        
        if value.unwrap().is_object() {
            current = value.unwrap();
        } else {
            break;
        }
    }
    current.clone()
}


#[derive(Debug)]
struct QueryCondition {
    key: String,
    value: String,
    op: String,
}

impl QueryCondition {
    fn new(key: String, value: String, op: String) -> Self {
        Self {
            key: key,
            value: value,
            op: op,
        }
    }
}

#[derive(Debug)]
struct Query {
    conditions: Vec<QueryCondition>,
}

impl Query {
    fn matches(&self, doc: &Value) -> bool {
        for condition in &self.conditions {
            let value = get_path(doc.clone(), &condition.key.split(".").map(|s| s.to_string()).collect::<Vec<String>>());
            if value.is_null() {
                return false;
            }
            let matches = match &condition.op[..] {
                "=" => value.as_str().unwrap() == &condition.value,
                ">" => if value.is_number() {
                    value.as_f64().unwrap() > condition.value.parse::<f64>().unwrap()
                } else {
                    return false;
                }
                "<" => if value.is_number() {
                    value.as_f64().unwrap() < condition.value.parse::<f64>().unwrap()
                } else {
                    return false;
                }
                _ => panic!("Invalid operator"),
            };
            if !matches {
                return false;
            }
        }
        true
    }
}



fn lex_string(input: &str) -> Result<(&str, &str), &str> {
    let input = input.trim_start();
    if input.starts_with('"') {
        let end = input[1..].find('"').ok_or("Expected end of quoted string")? + 1;
        let s = &input[1..end];
        let remaining = &input[end+1..];
        Ok((s, remaining))
    } else {
        let end = input.find(|c: char| !c.is_alphanumeric() && c != '.').unwrap_or_else(|| input.len());
        if end == 0 {
            Err("No string found")
        } else {
            Ok((&input[..end], &input[end..]))
        }
    }
}

fn parse_query(q: &str) -> Result<Query, &str> {
    let mut query = q.trim_start();
    let mut parsed = Query { conditions: vec![] };

    while !query.is_empty() {
        let (key, remaining) = lex_string(query)?;
        query = remaining.trim_start();

        if !query.starts_with(':') {
            return Err("Expected colon");
        }
        query = query[1..].trim_start();

        let op = match query.chars().next() {
            Some('>') | Some('<') => {
                let op = query[0..1].to_string();
                query = query[1..].trim_start();
                op
            },
            Some('=') => "=".to_string(),
            _ => return Err("Expected comparison operator"),
        };

        let (value, remaining) = lex_string(query)?;
        query = remaining.trim_start();

        let key = key.split('.').map(|s| s.to_owned()).collect();
        let argument = QueryCondition::new(key, value.to_owned(), op);
        parsed.conditions.push(argument);
    }

    Ok(parsed)
}


#[tokio::main]
async fn main() {
    let server = Arc::new(Server::new("docdb.data", "8080").unwrap());
    let port = server.port.clone();
    
    let server_clone = Arc::clone(&server);
    let add_document = warp::post()
        .and(warp::path("docs"))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&server_clone)))
        .and_then(|document, server: Arc<Server>| server.add_document(document));

    let server_clone = Arc::clone(&server);
    let get_document = warp::get()
        .and(warp::path("docs"))
        .and(warp::path::param())
        .and(warp::any().map(move || Arc::clone(&server_clone)))
        .and_then(|id, server: Arc<Server>| server.get_document(id));

    let server_clone = Arc::clone(&server);
    let search_documents = warp::get()
        .and(warp::path("docs"))
        .and(warp::query::<HashMap<String, String>>())
        .and(warp::any().map(move || Arc::clone(&server_clone)))
        .and_then(|query: HashMap<String, String>, server: Arc<Server>| {
            let q = query.get("q").unwrap_or(&"".to_string());
            server.search_documents(q);
        });

    let routes = add_document.or(get_document).or(search_documents);
    println!("Listening on port {}", port);
    warp::serve(routes).run(([127, 0, 0, 1], 8080)).await;
}