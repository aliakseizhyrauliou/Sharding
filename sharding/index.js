const app = require("express")();
const {Client} = require("pg");
const crypto = require("crypto");
const HashRing = require("hashring");

const hash = new HashRing();

hash.add("5432");
hash.add("5433");
hash.add("5434");

const clients = {
    "5432" : new Client({
        "host": "localhost",
        "port": "5432",
        "user": "postgres",
        "password": "password",
        "database": "postgres"
    }),
    "5433" : new Client({
        "host": "localhost",
        "port": "5433",
        "user": "postgres",
        "password": "password",
        "database": "postgres"
    }),
    "5434" : new Client({
        "host": "localhost",
        "port": "5434",
        "user": "postgres",
        "password": "password",
        "database": "postgres"
    }),
}

connect();

async function connect() {
    await clients["5432"].connect();
    await clients["5433"].connect();
    await clients["5434"].connect();
}

app.get("/:urlId", async (req, res) => {
    const urlId = req.params.urlId;
    const server = hash.get(urlId);

    const queryResult = await clients[server].query("SELECT * FROM url_table WHERE url_id = $1", [urlId]);    
    if(queryResult.rowCount > 0){
        res.send({
            "url_id": urlId,
            "url": queryResult.rows[0],
            "server": server
        })
    }
    else{
        res.sendStatus(404);
    }
})

app.post("/", async (req, res) => {
    const url = req.query.url;    
    //need to hash to get host
    const hash_url = crypto.createHash("sha256").update(url).digest("base64");

    const urlId = hash_url.substring(0,5);

    const server = hash.get(urlId);

    await clients[server].query("INSERT INTO url_table (url, url_id) VALUES ($1, $2)", [url, urlId]);

    res.send({
        "url_id": urlId,
        "url": url,
        "server": server
    })
})

app.listen(8081, () => console.log("Listening 8081"))