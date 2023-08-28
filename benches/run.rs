use criterion::{criterion_group, criterion_main, Criterion};
use tokio;

use owldb::db::Database;

fn database_insert_one_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = rt
        .block_on(Database::init("data_bench".to_string()))
        .unwrap();

    c.bench_function("db insert_one", |b| {
        b.iter(|| rt.block_on(db.insert_one("collection".to_string(), bson::doc! {"key": "value"})))
    });
}

criterion_group!(benches, database_insert_one_benchmark,);

criterion_main!(benches);
