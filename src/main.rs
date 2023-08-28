use env_logger::Builder;
use log::LevelFilter;

pub mod db;

const DB_FOLDER: &str = "data";

fn test_documents() -> Vec<bson::Document> {
    vec![
        bson::doc! {
            "name": "John",
            "age": 30
        },
        bson::doc! {
            "name": "Jane",
            "age": 25
        },
        bson::doc! {
            "name": "John",
            "age": 25
        },
    ]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Builder::new().filter(None, LevelFilter::Info).init();

    let mut database = db::Database::init(DB_FOLDER.to_string())
        .await
        .expect("Failed to initialize database");

    database.clear().await.expect("Failed to clear database");

    let documents = test_documents();
    for doc in documents.clone() {
        database
            .insert_one("users".to_string(), doc)
            .await
            .expect("Failed to insert document");
    }

    let all = database
        .find("users".to_string(), bson::doc! {})
        .await
        .expect("Failed to find all documents");

    assert_eq!(all.len(), documents.len());

    let found_docs = database
        .find("users".to_string(), bson::doc! { "name": "John" })
        .await
        .expect("Failed to find documents");

    assert_eq!(found_docs.len(), 2);

    Ok(())
}
