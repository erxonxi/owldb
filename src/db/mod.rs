pub enum DatabaseError {
    IoError(std::io::Error),
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
    ) -> Result<(), DatabaseError> {
        let mut buffer = Vec::new();
        doc.to_writer(&mut buffer)
            .map_err(|e| DatabaseError::BsonSerError(e))?;

        tokio::fs::write(format!("{}/{}.bson", self.folder_path, collection), &buffer)
            .await
            .map_err(|e| DatabaseError::IoError(e))?;

        Ok(())
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
}
