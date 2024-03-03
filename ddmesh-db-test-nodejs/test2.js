import {createClient} from "@libsql/client"

const client = createClient({
    url: "file:large_db.sqlite"
});

const encClient = createClient({
    url: "file:large_db_libsql_encrypted.db",
    encryptionKey: "demo",
});

// const httpClient = createClient({
//     url: "https://demo-bucket-oh.s3.us-east-2.amazonaws.com/large_db.sqlite?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEFkaCXVzLWVhc3QtMSJHMEUCIAqY7ASWjJTAjS1uAkkeubFHkR2shSryAMPzrcq8PtWqAiEA9RqqTAUZHrdJtwGcHokXEP02ZnNbThoSRkr6mxX3Q4EqygMIUhACGgwxODM0Njk0NjU4NTIiDNEOaRqbpAsD1mwK8iqnA%2BYFtXPx8gKmwpIDgaJs3KHyamQ5cQY45QhkIpJbwnLlUcw6Ybv7PhDYzEVnHUiwbZjxNmmAoNUAli9VGXwdNK9wUW8tO2KDVQuy7TRHKndBxgwA6VZdwE81eNw6Z67hCv4aa4REz3kcqRwLxZOPFtR8Iol2Lkxp4pQMn6hfZTDvvuuUYMr%2FspNRip46ERJb7TpInt4NMbJWUhViEC%2BpVxdVGdT1imWOqGvwl%2B6A%2FSMSDZyUhvchw6TnLPY7xWXfSlPCBNqJlmReC6tRFmkD%2Fiyvvw5yDpZqpWG9Ug7ez%2Bm5Jqr0%2BLEsHtRWNUqBgdrruPR6mPgAo3ZxPh3eimflh7i1P0IhM%2Bx8lVIoPHCUbhB6TmQHQSPcxXavs7tyzUxAOBYIAKJ8NP0%2F8kOTVSIRR2oS60c9yuEb%2FU4USOT8mprgLdZkXAg47mwip%2BUZUZYksaUmXtYoI37iFdICuDhDFHUjlRk1uh1G8TCOj3e7n2UwQf08GR6fryLFy%2Fr33TU9O2%2F7tcWFhskd9NJHl5z3zN4YTRoskbRd0qZoe7FAZkPqac9NNl6CJjDuhI%2BvBjqUAtOy87PW%2Bodk7Om8JDLudBHx9%2BY3KkppsT1OVxBRbE6jwuqj43%2BZsb0THHt%2FtOF3G4x5rSNvvlCHqE9P1wS3D9gxamGhUNzu7PC3ZN08EJFJLa90O1XgHiCxjfVqO69KX2XgDcvudRQIHKs4PcSOMXN9tcZF3fMabCj1P%2FmrQk5X%2BG2MLcKdOqsw0UcVZpmZX0Gi75zEss09biGnkCivjY8Tfml7uwuIqAFnZ8jKrYGn5ccn4MIiWpPEGoVGxngIYW5P5i70RcGO3jFUb1P0fSSFPnOzZynTvEp%2FCIMgavXW1tW8%2BgEs7FsMT9Mo6MkC63kRkHQOUoYetnTbXZ5haL%2Bn5rO9lQ0YPfaMo9%2BXP%2BXt7wOBAA%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20240303T004549Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIASVN5B4D6NZPLUXGN%2F20240303%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Signature=e049b1b668393c597b9368f9b26857b302fd1ad736a3e128ca2f287a7eb2df96"
// })
// const httpClouentEnc = createClient({});

const createTable = `
    CREATE TABLE IF NOT EXISTS Reviews
    (
        Id
        INTEGER
        PRIMARY
        KEY,
        ProductId
        TEXT,
        UserId
        TEXT,
        ProfileName
        TEXT,
        HelpfulnessNumerator
        INTEGER,
        HelpfulnessDenominator
        INTEGER,
        Score
        INTEGER,
        Time
        INTEGER,
        Summary
        TEXT,
        Text
        TEXT,
        EthAddress
        TEXT,
        PrivateKey
        TEXT
    )`;

const times = []
for(let i = 0; i < 10; i++) {
    console.time("unencrypted select all rows")
    const result = await client.execute("SELECT * FROM Reviews");
    console.timeEnd("unencrypted select all rows");
    times.push(result.time)
}
console.log("average time", times.reduce((a, b) => a + b, 0) / times.length)

const times2 = []
for(let i = 0; i < 10; i++) {
console.time("encrypted select all rows")
    const result = await encClient.execute("SELECT * FROM Reviews");
    console.timeEnd("encrypted select all rows");
    times2.push(result.time)
}
console.log("average time", times2.reduce((a, b) => a + b, 0) / times2.length)


const times4 = []
for(let i = 0; i < 10; i++) {
    console.time("unencrypted select rows with text 'good'")
    const result3 = await client.execute("SELECT * FROM Reviews WHERE Summary LIKE '%good%' OR Text LIKE '%good%'");
    console.timeEnd("unencrypted select rows with text 'good'");
times4.push(result3.time)
}
console.log("average time", times4.reduce((a, b) => a + b, 0) / times4.length)

const times3 = []
for(let i = 0; i < 10; i++) {
    console.time("encrypted select rows with text 'good'")
    const result4 = await encClient.execute("SELECT * FROM Reviews WHERE Summary LIKE '%good%' OR Text LIKE '%good%'");
    console.timeEnd("encrypted select rows with text 'good'");
    times3.push(result4.time)
}
console.log("average time", times3.reduce((a, b) => a + b, 0) / times3.length)
// let i = 0;
// for (const row of result.rows) {
//
//     const query = `INSERT INTO Reviews (Id, ProductId, UserId, ProfileName, HelpfulnessNumerator,
//                                         HelpfulnessDenominator, Score, Time, Summary, Text, EthAddress, PrivateKey)
//                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING;`
//
//     const rea2 = await encClient.execute({
//         sql: query,
//         args: [row.Id,
//             row.ProductId,
//             row.UserId,
//             row.ProfileName,
//             row.HelpfulnessNumerator,
//             row.HelpfulnessDenominator,
//             row.Score,
//             row.Time,
//             row.Summary,
//             row.Text,
//             row.EthAddress,
//             row.PrivateKey
//         ]
//     });
//     const rea2res = await encClient.execute({
//         sql: "SELECT * FROM Reviews WHERE Id = ?",
//         args: [rea2.lastInsertRowid]
//
//     });
//     i++;
//     if (i % 1000 === 0) {
//         console.log(rea2res)
//     }
// }


const encRes = await encClient.execute("SELECT * FROM Reviews");

for (let i = 0; i < 10; i++) {
    console.log(encRes.rows[i]);
}
console.log("encRes.rows.length", encRes.rows.length);