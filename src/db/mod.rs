use log::{error, info};

#[derive(Debug)]
pub enum DatabaseError {
    IoError(std::io::Error),
    BsonDeError(bson::de::Error),
    BsonSerError(bson::ser::Error),
}

pub struct Database {
    folder_path: String,
}

impl Database {
    pub fn create(folder_path: String) -> Self {
        Self { folder_path }
    }

    pub async fn insert_one(
        &self,
        collection: String,
        doc: bson::Document,
    ) -> Result<String, DatabaseError> {
        let id = bson::oid::ObjectId::new().to_string();
        let collection_path = self.get_collection_path(&collection);
        let full_path = self.get_document_path(&collection, &id);

        let mut buffer = Vec::new();
        doc.to_writer(&mut buffer)
            .map_err(|e| DatabaseError::BsonSerError(e))?;

        tokio::fs::create_dir_all(&collection_path)
            .await
            .map_err(|e| {
                error!("Failed to create collection directory: {}", e);
                DatabaseError::IoError(e)
            })?;

        tokio::fs::write(&full_path, &buffer).await.map_err(|e| {
            error!("Failed to write document: {}", e);
            DatabaseError::IoError(e)
        })?;

        info!("Document inserted with id: {}", id);

        Ok(id)
    }

    pub async fn find_one(
        &self,
        collection: String,
        id: String,
    ) -> Result<Option<bson::Document>, DatabaseError> {
        let path = self.get_document_path(&collection, &id);

        match tokio::fs::read(&path).await {
            Ok(buffer) => {
                let doc = bson::Document::from_reader(&buffer[..])
                    .map_err(|e| DatabaseError::BsonDeError(e))?;
                Ok(Some(doc))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => {
                error!("Failed to read document: {}", e);
                Err(DatabaseError::IoError(e))
            }
        }
    }

    fn get_collection_path(&self, collection: &String) -> String {
        format!("{}/{}", self.folder_path, collection)
    }

    fn get_document_path(&self, collection: &String, id: &String) -> String {
        format!("{}/{}.bson", self.get_collection_path(collection), id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_insert_one() {
        let db = Database::create("data_tests".to_string());

        let doc = bson::doc! {
            "name": "John",
            "age": 30
        };

        let res = db.insert_one("users".to_string(), doc).await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_find_one() {
        let db = Database::create("data_tests".to_string());

        let doc = bson::doc! {
            "name": "John",
            "age": 30
        };

        let id = db
            .insert_one("users".to_string(), doc.clone())
            .await
            .unwrap();

        let found_doc = db.find_one("users".to_string(), id.clone()).await;

        assert!(found_doc.is_ok());

        let found_doc = found_doc.unwrap();

        assert!(found_doc.is_some());

        let found_doc = found_doc.unwrap();

        assert_eq!(found_doc, doc);
    }
}
