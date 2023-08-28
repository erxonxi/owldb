use std::collections::{HashMap, HashSet};

use log::{error, info};

#[derive(Debug)]
pub enum DatabaseError {
    IoError(std::io::Error),
    BsonDeError(bson::de::Error),
    BsonSerError(bson::ser::Error),
}

pub struct Database {
    folder_path: String,
    index: HashMap<String, HashMap<String, Vec<String>>>, // colección -> campo -> [IDs]
}

impl Database {
    pub async fn init(folder_path: String) -> Result<Self, DatabaseError> {
        info!(
            "Successfully initialized database at directory: {}",
            folder_path
        );

        let index = HashMap::new();
        let db = Self { folder_path, index };
        db.create_path_dirs(&db.folder_path).await?;

        Ok(db)
    }

    #[cfg(test)]
    async fn init_test(folder_path: String, id: String) -> Self {
        let db = Self {
            folder_path: format!("{}/{}", folder_path, id),
            index: HashMap::new(),
        };
        db.create_path_dirs(&db.folder_path).await.unwrap();
        db
    }

    pub async fn clear(&self) -> Result<(), DatabaseError> {
        tokio::fs::remove_dir_all(&self.folder_path)
            .await
            .map_err(|e| {
                error!("Error removing database directory: {}", e);
                DatabaseError::IoError(e)
            })?;

        self.create_path_dirs(&self.folder_path).await?;

        Ok(())
    }

    pub fn add_index(&mut self, collection: String, field: String) {
        if let Some(field_index) = self.index.get_mut(&collection) {
            if !field_index.contains_key(&field) {
                field_index.insert(field, Vec::new());
            }
        } else {
            let mut field_index = HashMap::new();
            field_index.insert(field, Vec::new());
            self.index.insert(collection, field_index);
        }
    }

    pub async fn insert_one(
        &mut self,
        collection: String,
        doc: bson::Document,
    ) -> Result<String, DatabaseError> {
        let id = bson::oid::ObjectId::new().to_string();
        let collection_path = self.get_collection_path(&collection);
        let full_path = self.get_document_path(&collection, &id);

        let mut buffer = Vec::new();
        doc.to_writer(&mut buffer)
            .map_err(|e| DatabaseError::BsonSerError(e))?;

        self.create_path_dirs(&collection_path).await?;

        tokio::fs::write(&full_path, &buffer).await.map_err(|e| {
            error!("Failed to write document: {}", e);
            DatabaseError::IoError(e)
        })?;

        if let Some(field_index) = self.index.get_mut(&collection) {
            for (field, _) in doc.iter() {
                if let Some(ids) = field_index.get_mut(field) {
                    ids.push(id.clone());
                } else {
                    field_index.insert(field.clone(), vec![id.clone()]);
                }
            }
        }

        info!(
            "Successfully inserted document into '{}' with ID: '{}'",
            collection, id
        );

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

    pub async fn find(
        &self,
        collection: String,
        query: bson::Document,
    ) -> Result<Vec<bson::Document>, DatabaseError> {
        let collection_path = self.get_collection_path(&collection);
        let mut results = Vec::new();

        if let Some(field_index) = self.index.get(&collection) {
            // Filtro los IDs que coinciden con la consulta.
            let mut candidate_ids: Option<HashSet<String>> = None;

            for (field, _) in query.iter() {
                if let Some(ids) = field_index.get(field) {
                    let ids_set: HashSet<String> = ids.clone().into_iter().collect();

                    if let Some(existing_set) = candidate_ids.as_mut() {
                        *existing_set = existing_set.intersection(&ids_set).cloned().collect();
                    } else {
                        candidate_ids = Some(ids_set);
                    }
                }
            }

            if let Some(ids) = candidate_ids {
                for id in ids {
                    let doc = self.find_one(collection.clone(), id).await?;
                    if let Some(doc) = doc {
                        results.push(doc);
                    }
                }
            }

            return Ok(results);
        }

        let mut entries = tokio::fs::read_dir(collection_path).await.map_err(|e| {
            error!("Failed to read collection directory: {}", e);
            DatabaseError::IoError(e)
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            error!("Failed to read next entry: {}", e);
            DatabaseError::IoError(e)
        })? {
            let path = entry.path();
            let buffer = tokio::fs::read(&path).await.map_err(|e| {
                error!("Failed to read document: {}", e);
                DatabaseError::IoError(e)
            })?;

            let doc = bson::Document::from_reader(&buffer[..])
                .map_err(|e| DatabaseError::BsonDeError(e))?;

            if query.iter().all(|(k, v)| doc.get(k) == Some(v)) {
                results.push(doc);
            }
        }

        Ok(results)
    }

    pub async fn delete_one(
        &self,
        collection: String,
        id: String,
    ) -> Result<Option<bson::Document>, DatabaseError> {
        let path = self.get_document_path(&collection, &id);

        match tokio::fs::remove_file(&path).await {
            Ok(_) => {
                info!(
                    "Successfully deleted document from '{}' with ID: '{}'",
                    collection, id
                );
                Ok(None)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                info!("Document not found in '{}' with ID: '{}'", collection, id);
                Ok(None)
            }
            Err(e) => {
                error!("Failed to delete document: {}", e);
                Err(DatabaseError::IoError(e))
            }
        }
    }

    pub async fn delete(
        &self,
        collection: String,
        query: bson::Document,
    ) -> Result<Vec<String>, DatabaseError> {
        let collection_path = self.get_collection_path(&collection);
        let mut deleted_ids = Vec::new();

        let mut entries = tokio::fs::read_dir(collection_path).await.map_err(|e| {
            error!("Failed to read collection directory: {}", e);
            DatabaseError::IoError(e)
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            error!("Failed to read next entry: {}", e);
            DatabaseError::IoError(e)
        })? {
            let path = entry.path();
            let buffer = tokio::fs::read(&path).await.map_err(|e| {
                error!("Failed to read document: {}", e);
                DatabaseError::IoError(e)
            })?;

            let doc = bson::Document::from_reader(&buffer[..])
                .map_err(|e| DatabaseError::BsonDeError(e))?;

            if query.iter().all(|(k, v)| doc.get(k) == Some(v)) {
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    error!("Failed to delete document: {}", e);
                    return Err(DatabaseError::IoError(e));
                }
                let id = path.file_stem().unwrap().to_str().unwrap().to_string();
                deleted_ids.push(id.clone());
                info!(
                    "Successfully deleted document from '{}' with ID: '{}'",
                    collection, id
                );
            }
        }

        Ok(deleted_ids)
    }

    fn get_collection_path(&self, collection: &String) -> String {
        format!("{}/{}", self.folder_path, collection)
    }

    fn get_document_path(&self, collection: &String, id: &String) -> String {
        format!("{}/{}.bson", self.get_collection_path(collection), id)
    }

    async fn create_path_dirs(&self, path: &String) -> Result<(), DatabaseError> {
        tokio::fs::create_dir_all(path).await.map_err(|e| {
            error!("Failed to create directory: {}", e);
            DatabaseError::IoError(e)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_insert_one() {
        let mut db = Database::init("data_tests".to_string()).await.unwrap();

        let doc = bson::doc! {
            "name": "John",
            "age": 30
        };

        let res = db.insert_one("users".to_string(), doc).await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_find_one() {
        let mut db = Database::init("data_tests".to_string()).await.unwrap();

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

    #[tokio::test]
    async fn test_find() {
        let mut db = Database::init_test("data_tests".to_string(), "test_find".to_string()).await;
        db.clear().await.unwrap();

        let documents = test_documents();
        for doc in documents.clone() {
            db.insert_one("users".to_string(), doc)
                .await
                .expect("Failed to insert document");
        }

        let found_docs = db
            .find("users".to_string(), bson::doc! { "name": "John" })
            .await
            .expect("Failed to find documents");

        assert_eq!(found_docs.len(), 2);

        for doc in found_docs {
            assert!(documents.contains(&doc));
        }
    }

    #[tokio::test]
    async fn test_find_filtered() {
        let mut db =
            Database::init_test("data_tests".to_string(), "test_find_filtered".to_string()).await;
        db.clear().await.unwrap();

        let documents = test_documents();
        for doc in documents.clone() {
            db.insert_one("users".to_string(), doc)
                .await
                .expect("Failed to insert document");
        }

        let found_docs = db
            .find(
                "users".to_string(),
                bson::doc! { "name": "John", "age": 25 },
            )
            .await
            .expect("Failed to find documents");

        assert_eq!(found_docs.len(), 1);

        for doc in found_docs {
            assert!(documents.contains(&doc));
        }
    }

    #[tokio::test]
    async fn test_delete_one() {
        let mut db =
            Database::init_test("data_tests".to_string(), "test_delete_one".to_string()).await;

        db.clear().await.unwrap();

        let documents = test_documents();

        let id = db
            .insert_one("users".to_string(), documents[0].clone())
            .await
            .expect("Failed to insert document");

        let deleted_doc = db
            .delete_one("users".to_string(), id.clone())
            .await
            .expect("Failed to delete document");

        assert!(deleted_doc.is_none());

        let found_doc = db
            .find_one("users".to_string(), id.clone())
            .await
            .expect("Failed to find document");

        assert!(found_doc.is_none());
    }

    #[tokio::test]
    async fn test_delete() {
        let mut db = Database::init_test("data_tests".to_string(), "test_delete".to_string()).await;

        db.clear().await.unwrap();

        let documents = test_documents();

        for doc in documents.clone() {
            db.insert_one("users".to_string(), doc)
                .await
                .expect("Failed to insert document");
        }

        let deleted_ids = db
            .delete("users".to_string(), bson::doc! { "name": "John" })
            .await
            .expect("Failed to delete documents");

        assert_eq!(deleted_ids.len(), 2);

        for id in deleted_ids {
            let found_doc = db
                .find_one("users".to_string(), id.clone())
                .await
                .expect("Failed to find document");

            assert!(found_doc.is_none());
        }
    }

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
}
