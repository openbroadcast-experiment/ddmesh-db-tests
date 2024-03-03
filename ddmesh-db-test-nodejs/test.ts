import { createClient } from "https://esm.sh/@libsql/client@0.3.5/web";

const config = {
    url: "file:large_db.sqlite"
};

const client = createClient(config);

const result = await client.execute("SELECT * FROM Reviews");
console.log(result.rows.length);