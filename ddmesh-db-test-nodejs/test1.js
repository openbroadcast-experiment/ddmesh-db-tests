import {createClient} from "@libsql/client"


const client2 = createClient({
    url: "file:large_db2.db"
});

const encClient = createClient({
    url: "file:large_db_libsql_encrypted.db",
    encryptionKey: "demo",
});

const createTable2 = `CREATE TABLE IF NOT EXISTS Users
                     (
                         Id
                         INTEGER
                         PRIMARY
                         KEY,
                        name
                         TEXT
                     )`
await client2.execute(createTable2);
await encClient.execute(createTable2);

console.log("result.rows.length", result.rows.length);
for (const row of result.rows) {
    const query = `INSERT INTO Reviews (Id, ProductId, UserId, ProfileName, HelpfulnessNumerator,
                                        HelpfulnessDenominator, Score, Time, Summary, Text, EthAddress, PrivateKey)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING;`
    // const rea = await encClient.execute(query,
    //     [row.Id,
    //         row.ProductId,
    //         row.UserId,
    //         row.ProfileName,
    //         row.HelpfulnessNumerator,
    //         row.HelpfulnessDenominator,
    //         row.Score,
    //         row.Time,
    //         row.Summary,
    //         row.Text,
    //         row.EthAddress,
    //         row.PrivateKey]
    // );

    const query2 = `INSERT INTO Reviews (Id, ProductId, UserId, ProfileName, HelpfulnessNumerator,
                                        HelpfulnessDenominator, Score, Time, Summary, Text, EthAddress, PrivateKey)
                   VALUES ("${row.Id}", "${row.ProductId}", "${row.UserId}", "${row.ProfileName}", "${row.HelpfulnessNumerator}", "${row.HelpfulnessDenominator}", 
                           "${row.Score}", "${row.Time}", "${row.Summary}", "${row.Text}", "${row.EthAddress}","${row.PrivateKey}" ) ON CONFLICT DO NOTHING;`
    // const rea = await encClient.execute(query,

    const rea2 = await client2.execute({
        sql: query,
        args: [row.Id,
        row.ProductId,
        row.UserId,
        row.ProfileName,
        row.HelpfulnessNumerator,
        row.HelpfulnessDenominator,
        row.Score,
        row.Time,
        row.Summary,
        row.Text,
        row.EthAddress,
        row.PrivateKey
]
});

    const res3 = await client2.execute({
        sql: "INSERT INTO Users (name) VALUES (?)",
        args: ["abc"]
    } );

    const res4 = await encClient.execute({
        sql: "INSERT INTO Users (name) VALUES (?)",
        args: ["abc"]
    } );
    // console.log(query2)
    // const rea4 =  await client2.execute(query2);
    // console.log(row.Id, row.ProductId, row.UserId, row.ProfileName, row.HelpfulnessNumerator, row.HelpfulnessDenominator, row.Score, row.Time, row.Summary, row.Text, row.EthAddress, row.PrivateKey);
    // console.log(rea);
    console.log(rea2);
    console.log(res3);
    console.log(res4);
}


const encRes = await encClient.execute("SELECT * FROM Reviews");

for (let i = 0; i < 10; i++) {
    console.log(encRes.rows[i]);
}
console.log("encRes.rows.length", encRes.rows.length);