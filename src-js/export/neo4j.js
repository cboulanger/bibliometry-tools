"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const neo4j_driver_1 = require("neo4j-driver");
const couchbase = require("couchbase");
const createDebugger = require("debug");
const process = require("process");
const debug = createDebugger('neo4j');
const Gauge = require('gauge');
const gauge = new Gauge;
async function neo4j_export() {
    const couchbase_cluster = await couchbase.connect(`couchbase://${process.env.COUCH_REMOTE_URL}`, {
        username: process.env.COUCH_REMOTE_USER,
        password: process.env.COUCH_REMOTE_PASSWD,
    });
    const zoteroGroupId = "g2829873";
    const couchbaseKeyspace = `zotero.${zoteroGroupId}.items`;
    /**
     * Execute N1QL statement in Couchbase server
     */
    async function couchbase_execute(query) {
        let rows;
        let attempts = 3;
        let a = attempts;
        while (a > 0) {
            try {
                ({ rows } = await couchbase_cluster.query(query.replace(/ +/g, " ")));
                return rows;
            }
            catch (e) {
                if (--a === 0) {
                    throw e;
                }
                console.error(`Getting "${e.message}" from couchbase server, retrying ${a} times ... `);
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
        throw new Error(`Could not reach Couchbase server after ${attempts} attempts.`);
    }
    // intialize neo4j session
    const neo4j_driver = neo4j_driver_1.default.driver(process.env.NEO4J_URL, neo4j_driver_1.default.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD));
    /**
     * Execute cypher statement in Neo4J server
     */
    async function neo4j_execute(statement, params = {}) {
        const neo4j_session = neo4j_driver.session({
            database: process.env.NEO4J_DATABASE,
            defaultAccessMode: neo4j_driver_1.default.session.WRITE
        });
        return new Promise((resolve, reject) => {
            // noinspection TypeScriptValidateJSTypes
            let result = neo4j_session.run(statement, params)
                .subscribe({
                onNext: record => {
                    debug("Created record " + record.get('name'));
                },
                onCompleted: () => {
                    neo4j_session.close().then(resolve);
                },
                onError: error => {
                    reject(error);
                }
            });
        });
    }
    // create constraints & indexes
    // language=cypher
    let query = `CREATE CONSTRAINT zotero_key IF NOT EXISTS ON (p:Publication) ASSERT p.zotero_key IS UNIQUE;
    CREATE INDEX source_title IF NOT EXISTS FOR (s:Source) ON (s.title);
    CREATE INDEX author_lastname IF NOT EXISTS FOR (a:Author) ON (a.lastName);
    CALL db.awaitIndexes();`;
    for (let line of query.split('\n')) {
        await neo4j_execute(line);
    }
    /**
     * Merges a Publication node based on the Zotero key, plus
     * connected Source and Author nodes, plus cited
     * Publication nodes
     */
    async function mergePublicationNode(item, indent = 0) {
        // create/update
        let { key, DOI = "", title, date, itemType, publicationTitle } = item;
        debug(`${" ".repeat(indent)}Merging publication & source nodes for ${title}`);
        if (publicationTitle) {
            // language=cypher
            await neo4j_execute(`
        MERGE (p:Publication {zotero_key: $key})
        ON CREATE SET
          p.dataSource  = "zotero",
          p.DOI         = $DOI,
          p.title       = $title,
          p.date        = $date,
          p.itemType    = $itemType
        MERGE (s:Source {title: $publicationTitle})
        ON CREATE SET
          s.canonicalTitle = $canonicalTitle
        MERGE (p)-[:PUBLISHED_IN]->(s);
      `, {
                key, DOI, title, date, itemType,
                publicationTitle: publicationTitle.toLocaleLowerCase(),
                canonicalTitle: publicationTitle
            });
        }
        else {
            // language=cypher
            await neo4j_execute(`
        MERGE (p:Publication {zotero_key: $key})
        ON CREATE SET
          p.dataSource  = "zotero",
          p.DOI         = $DOI,
          p.title       = $title,
          p.date        = $date,
          p.itemType    = $itemType;
      `, {
                key, DOI, title, date, itemType
            });
        }
        // authors
        for (let creator of item.creators) {
            let { lastName, firstName = "", creatorType, name } = creator;
            if (!lastName) {
                if (name) {
                    lastName = name;
                }
                else
                    continue;
            }
            if (!name) {
                name = firstName + " " + lastName;
            }
            debug(`${" ".repeat(indent)}Merging author node for ${name}`);
            // language=cypher
            await neo4j_execute(`
        MERGE (a:Author {lastName: $lastName, firstName: $firstName})
        WITH a 
        MATCH (p:Publication {zotero_key: $key})
        MERGE (a)-[:IS_CREATOR_OF {creatorType: $creatorType}]->(p);
      `, {
                key, firstName, lastName, name, creatorType
            });
        }
    }
    // get all keys of items that have citation
    const bucket = couchbase_cluster.bucket("zotero");
    // language=n1ql
    let keys = await couchbase_execute(`
    SELECT RAW meta(g).id 
    FROM ${couchbaseKeyspace} g 
    WHERE g.relations.\`dc:relation\` is not null;
  `);
    // Import nodes into Neo4J
    let count = 0;
    let total = keys.length;
    for (let key of keys) {
        let item = (await bucket.scope(zoteroGroupId).collection("items").get(key)).content;
        await mergePublicationNode(item);
        gauge.show(`Importing '${item.title.substr(0, 30)}' (${++count}/${total})...`, count / total);
        let r = item.relations['dc:relation'];
        let ref_uris = Array.isArray(r) ? r : [r];
        for (let ref_uri of ref_uris) {
            // https://api.zotero.org/groups/1234/items/BFS4D2
            let [, , , libraryType, libraryId, , ref_key] = ref_uri.split('/');
            libraryId = libraryType[0] + libraryId;
            // skip citing item
            if (ref_key == key)
                continue;
            let ref_item;
            try {
                ref_item = (await bucket.scope(libraryId).collection("items").get(ref_key)).content;
            }
            catch (e) {
                if (e.message.includes('document not found')) {
                    debug(`Item with ${ref_uri} could not be found in couchbase cache`);
                    continue;
                }
                throw e;
            }
            await mergePublicationNode(ref_item, 4);
            // language=cypher
            await neo4j_execute(`
        MATCH (p1:Publication {zotero_key: $key})
        MATCH (p2:Publication {zotero_key: $ref_key})
        MERGE (p1)-[:CITES]->(p2);
      `, { key, ref_key });
        }
    }
    gauge.hide();
    await neo4j_driver.close();
}
exports.default = neo4j_export;
