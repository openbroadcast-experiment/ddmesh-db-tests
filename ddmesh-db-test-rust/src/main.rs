use ethers::core::rand::thread_rng;
use ethers::prelude::*;
use libsql::Database;
use secp256k1::Secp256k1;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::Connection;
use sqlx::{Row, SqliteConnection};

#[derive(Debug)]
struct Record {
    Id: i64,
    ProductId: String,
    UserId: String,
    ProfileName: String,
    HelpfulnessNumerator: i64,
    HelpfulnessDenominator: i64,
    Score: i64,
    Time: i64,
    Summary: String,
    Text: String,
    PrivateKey: String,
    EthAddress: String,
}

async fn populate_null_cols() -> Result<(), sqlx::Error> {
    let db = SqlitePoolOptions::new()
        .max_connections(20)
        .connect("sqlite://./large_db.sqlite")
        .await?;

    let rows = sqlx::query("SELECT * FROM Reviews").fetch_all(&db).await?;

    let mut rng = thread_rng();
    let secp = Secp256k1::new();
    //For each record, if PrivateKey or EthAddress is null, populate with random data
    for row in rows {
        let mut record = Record {
            Id: row.get(0),
            ProductId: row.get(1),
            UserId: row.get(2),
            ProfileName: row.get(3),
            HelpfulnessNumerator: row.get(4),
            HelpfulnessDenominator: row.get(5),
            Score: row.get(6),
            Time: row.get(7),
            Summary: row.get(8),
            Text: row.get(9),
            EthAddress: row.get(10),
            PrivateKey: row.get(11),
        };
        // println!("Record: {:?}", record);
        if record.PrivateKey.is_empty() || record.EthAddress.is_empty() {
            let (secretkey, pubkey) = secp.generate_keypair(&mut rng);
            // println!(
            //     "Generated keypair: (Private: {:?}, Public: {:?})",
            //     secretkey, pubkey
            // );
            record.PrivateKey = hex::encode(secretkey.as_ref().to_vec());
            record.EthAddress = hex::encode(pubkey.serialize().to_vec());

            let rows_affected =
                sqlx::query("UPDATE Reviews SET PrivateKey = ?, EthAddress = ? WHERE Id = ?")
                    .bind(&record.PrivateKey)
                    .bind(&record.EthAddress)
                    .bind(&record.Id)
                    .execute(&db)
                    .await?
                    .rows_affected();
        }
    }
    Ok(())
}
async fn copy_to_libsql_encrypted() -> Result<(), sqlx::Error> {
    let db = SqlitePoolOptions::new()
        .max_connections(20)
        .connect("sqlite://./large_db.sqlite")
        .await?;

    let rows = sqlx::query("SELECT * FROM Reviews").fetch_all(&db).await?;
    let encDb = Database::open("sqlite://./encrypted_db.sqlite").unwrap();
    encDb.Ok(())
    Database::open_with_flags("sqlite://./encrypted_db.sqlite", )
}

#[tokio::main]
async fn main() {
    populate_null_cols().await.unwrap();
    println!("Hello, world!");
}
