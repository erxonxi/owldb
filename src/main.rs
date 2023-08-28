use env_logger::Builder;
use log::LevelFilter;

mod db;

const DB_FOLDER: &str = "data";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Builder::new().filter(None, LevelFilter::Info).init();

    let database = db::Database::create(DB_FOLDER.to_string());

    let doc = bson::doc! {
        "name": "John",
        "age": 30
    };

    let res = database.insert_one("users".to_string(), doc).await;

    assert!(res.is_ok());

    Ok(())
}
