const sandbox = require("../sandbox");
const process = require('process');
const zoteroApi = sandbox.zoteroApi; // TODO
const libraryApi = zoteroApi.getLibraryApi();
const deGruyterJats     = require('../import/deGruyterJats');
const digiZeitschriften = require('../import/digiZeitschriften');
const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');

/**
 * Normalizes data from backends to the zotero data model format and stores saves it
 * locally and to zotero.org
 * @param refresh
 * @returns {Promise<void>}
 */
async function normalize(refresh=false) {
  const cluster = await sandbox.getLocalCluster();
  const buckets = sandbox.buckets;
  const articlesBucket = cluster.bucket(buckets.ARTICLES).defaultCollection();

  // load all articles to be imported into memory - this should be rewritten to load on demand
  let items = []; //
  for (let source of [/* deGruyterJats, */digiZeitschriften]) {
    items = items.concat(await source.toZotero());
  }

  let count = 0;
  let total = items.length;
  if (total===0) return;
  let response;
  const batchSize = 20;
  let queue = [];
  let issueKeys = {};

  for (let item of items) {

    count++;
    if(count<488) continue;
    // ignore non-article entries
    if (!item.publicationTitle) {
      console.warn("Skipping item without 'publicationTitle' field...");
      continue;
    }

    let zoteroItem;
    let localItem;
    let foundLocally = false;
    let foundOnZoteroOrg = false;
    let isSame = false;

    function progress(msg){
      let section = `${count}/${total} '${item.title.substr(0, 60)}': ${msg}`;
      //sandbox.gauge.pulse();
      //sandbox.gauge.show(section, total/count);
      console.log(section);
    }
    // check if entry exists and try to get zotero item key
    if (item.DOI) {
      try {
        localItem = (await articlesBucket.get(item.DOI)).content;
        foundLocally = true;
      } catch(e) {}
      // try again
      if (!foundLocally && item.DOI.startsWith("oai:")) {
        let titleWords = item.title
          .replace(/[,.\-:"'?]/g," ")
          .split(" ")
          .filter(str => Boolean(str))
          .sort((a, b) => b.length - a.length);
        progress(`Searching for ${titleWords.slice(0,3).join(" ")} ${item.date}`);
        let query = `
        SELECT raw meta().id FROM ${buckets.ARTICLES}
        WHERE publicationTitle like '${item.publicationTitle}'
          AND title LIKE '%${titleWords[0]}%'
          AND title LIKE '%${titleWords[1]}%'
          AND title LIKE '%${titleWords[2]}%'
          AND \`date\` LIKE '${item.date}%'`;
        let ids = (await cluster.query(query)).rows;
        if (ids.length > 0) {
          let id = ids.shift();
          localItem = (await articlesBucket.get(id)).content;
          foundLocally = true;
        }
      }
    }

    if (foundLocally) {
      if (item.DOI.startsWith("oai:")) {
        // overwrite incomplete item with saved values except callNumber field
        if (item.callNumber) {
          delete localItem.callNumber;
        }
        item = Object.assign(item, localItem);
        delete item.version;
        delete item.dateAdded;
        delete item.dateModified;
      } else {
        item.key = localItem.key;
      }
    }

    if (refresh) {
      progress(`Updating item locally and on zotero.org...`);
    } else if (foundLocally) {
      isSame = Object.entries(item).every(([key, value]) => key === "collections" || sandbox.isDeepEqual(value, localItem[key]));
      if (isSame) {
        progress(`Local copy exists and is identical - assuming it was also created on zotero.org.`);
        continue;
      } else {
        progress(`Local copy exists but needs to be updated.`);
      }
    } else {
      progress(`Not stored locally.`);
    }

    // collection
    let collectionKey;
    if (localItem) {
      item.collections = localItem.collections;
      collectionKey = item.collections[0];
    } else {
      // determine the key of the collection into which to place the item
      let collectionKey;

      // collection for the publication
      if (item.publicationTitle != issueKeys.publicationTitle ) {
        issueKeys = {
          publicationTitle: item.publicationTitle
        };
      }
      let publicationCollectionKey = issueKeys.publicationCollectionKey;
      if (!publicationCollectionKey) {
        publicationCollectionKey = await zoteroApi.createCollection(item.publicationTitle);
        issueKeys.publicationCollectionKey = publicationCollectionKey;
      }

      // subcollection for the volume
      let volumeKey;
      if (item.volume) {
        if (!issueKeys[item.volume]) {
          issueKeys[item.volume] = {};
        }
        volumeKey = issueKeys[item.volume].volume;
        if (!volumeKey) {
          let volume = item.volume.padStart(2, "0");
          volumeKey = await zoteroApi.createCollection(`Volume ${volume}`, publicationCollectionKey);
          issueKeys[item.volume].volume = volumeKey;
        }
      }

      // subcollection for the issue, if it exists
      if (volumeKey && item.issue) {
        collectionKey = issueKeys[item.volume][item.issue];
        if (!collectionKey) {
          collectionKey = await zoteroApi.createCollection(`Issue ${item.issue}`, volumeKey);
          issueKeys[item.volume][item.issue] = collectionKey;
        }
      }

      // fallback
      if (!collectionKey) {
        collectionKey = volumeKey || publicationCollectionKey;
      }

      item.collections = [collectionKey];
    }
    let retries = 3
    for (let t=1; t <= retries; t++) {
      try {
        progress(`Retrieving from zotero.org ${t>1? " #"+t:""}...`);
        if (item.key) {
          response = await libraryApi.items(item.key).get();
          zoteroItem = response.getData();
        } else {
          response = await libraryApi.collections(collectionKey).items().get();
          zoteroItem = response.getData().find(r => r.DOI === item.DOI);
        }
        if (zoteroItem) {
          item.key = zoteroItem.key;
          foundOnZoteroOrg = true;
        }
        break;
      } catch (e) {
        if (e.message.includes("Not Found")) {
          break;
        }
        if (!e.message.includes("ETIMEDOUT")){
          throw e;
        }
        console.error(e.message);
        if (t === retries) {
          throw new Error("Too many retries.");
        }
      }
    }

    // update item on zotero.org
    if (foundOnZoteroOrg) {
      delete item.relations;
      delete item.collections;
      let changedEntries = Object.fromEntries(
        Object.entries(item)
          .filter(([key, value]) => Boolean(value) && !sandbox.isDeepEqual(value, zoteroItem[key]))
      );
      let changedKeys = Object.keys(changedEntries);
      if (changedKeys.length > 0) {
        changedEntries.version = zoteroItem.version;
        for (let t=1; t <= retries; t++) {
          try {
            await libraryApi.items(item.key).patch(changedEntries);
            break;
          } catch (e) {
            if (e.reason) {
              console.warn(e.reason);
              break;
            } else if (!e.message.includes("ETIMEDOUT")){
              throw e;
            }
            console.error(e.message);
            if (t === retries) {
              throw new Error("Too many retries.");
            }
          }
        }
        progress(`Updated field(s) '${changedKeys.join("', '")}' on zotero.org.`);
        zoteroItem = Object.assign(zoteroItem, changedEntries);
      } else {
        progress(`Item on zotero.org is up-to-date.`);
      }
      if (!foundLocally || !isSame) {
        await articlesBucket.upsert(item.DOI, zoteroItem);
        progress("Saved updated zotero item locally.");
      }
      continue;
    }

    // create new zotero items
    if (!item.relations) {
      item.relations = {};
    }
    queue.push(item);
    if (queue.length < batchSize && count < total) {
      progress(`Added to queue to be uploaded to zotero.org (${queue.length})`);
      continue;
    }
    console.log(`>>> Creating ${batchSize} items on zotero.org...`);
    response = await libraryApi.items().post(queue);
    if (response.isSuccess()) {
      queue = [];
      console.log(`>>> Upload successful. Saving ${batchSize} items locally...`);
      for (zoteroItem of response.getData()) {
        await articlesBucket.upsert(item.DOI, zoteroItem);
      }
      continue;
    }
    console.error("Zotero server returned one or more errors: " +
      JSON.stringify(response.getErrors(), null, 2) +
      "\nPayload was: " +
      JSON.stringify(queue, null, 2)
    );
    process.exit(1);
  }
}

const nodeFetch = require('node-fetch')
const fetch = require('fetch-cookie/node-fetch')(nodeFetch);
const {URLSearchParams} = require('url');
const params = new URLSearchParams({
  user: sandbox.ENV.EZPROXY_USER,
  pass: sandbox.ENV.EZPROXY_PASS,
  login: "Login"
});
const ezproxyUrlPrefix = "https://login.ezproxy.rg.mpg.de/login?qUrl=";

async function downloadViaEZProxy(url, filePath) {
  // login with credentials
  let res = await fetch(ezproxyUrlPrefix + url, {method: 'POST', body: params});
  // first redirection to page containing the final url
  res = await fetch(res.url);
  let contentType = res.headers.get("content-type").split(";").shift();
  let size = 0;
  switch (contentType) {
    case "text/html":
      // intermediary page or no access page
      let html = await res.text();
      if (html.includes("You currently have no access")) {
        throw new Error("No access");
      }
      let match = html.match(/href="([^"]+)"/);
      if (Array.isArray(match)) {
        url = match[1];
      } else {
        fs.writeFileSync("out/invalid-response.html", html, "utf8");
        throw new Error("Invalid html response");
      }
      // re-fetch from new url
      res = await fetch(url);
      // fallthrough
    case "application/pdf":
      // download pdf document
      await new Promise(((resolve, reject) => {
        res.body
          .on('data', chunk => size += chunk.length)
          .pipe(fs.createWriteStream(filePath))
          .on('finish', resolve);
      }));
      break;
    default:
      throw new Error("Unexpeced response of type " + contentType)
  }
  return size;
}

async function download() {
  const filesize = require('filesize');
  const filepathPrefix = "data/documents/";
  const cluster = await sandbox.getLocalCluster();
  let {rows} = await cluster.query("SELECT meta().id, callNumber, title FROM articles;");
  let counter = 0;
  let total = rows.length;
  let failedDownloads = "";
  const failedDownloadsFile = "out/failed-downloads.txt";
  try {
    failedDownloads = fs.readFileSync(failedDownloadsFile, "utf8");
  } catch (e) {}
  let size = 0;
  try {
    for (let row of rows) {
      counter++;
      let urls = [];
      let filename;
      if (row.id.startsWith("10.")) {
        urls.push(deGruyterJats.getDownloadUrl(row.id));
        filename = row.id.replace("/","_") + ".pdf";
        if (row.callNumber) {
          urls.push(digiZeitschriften.getDownloadUrl(row.callNumber));
        }
      } else {
        urls.push(digiZeitschriften.getDownloadUrl(row.id));
        filename = row.id + ".pdf";
      }
      let filepath = filepathPrefix + filename;
      let msg = `${counter}/${total} (${filesize(size)})`.padEnd(20)  + row.title.substr(0,50);

      sandbox.gauge.show(msg, counter/total);

      // we have that file already
      if (fs.existsSync(filepath)) continue;

      // we know we have no access
      if (failedDownloads.includes(row.id)) continue;

      let error;
      let url;
      let pulseId;
      for (url of urls){
        for (let i=0; i < 5; i++) {
          error = null;
          if (pulseId) clearInterval(pulseId);
          sandbox.gauge.pulse();
          pulseId = setInterval(sandbox.gauge.pulse.bind(sandbox.gauge), 100, `Trying download from ${url} (${i+1})`);
          try {
            size += await downloadViaEZProxy(url, filepath);
          } catch (e) {
            error = e;
            if (e.code === "ETIMEDOUT") {
              clearInterval(pulseId);
              sandbox.gauge.pulse();
              pulseId = setInterval(sandbox.gauge.pulse.bind(sandbox.gauge), 100, "Request timed out, retrying after 10s");
              await new Promise(resolve => setTimeout(resolve, 10000));
              continue; // retry download
            }
            if (e.message !== "No access") {
              throw e;
            }
          }
          break; // retry download
        }
        if (!error) break; // url loop
      }
      clearInterval(pulseId);
      if (error) {
        let line = `\n${msg.padEnd(70, " ")} | ${error.message.substr(0,20)} | ${url} | ${row.id} | ${row.callNumber ? " | " + row.callNumber:""}`;
        await fsp.appendFile(failedDownloadsFile, line, "utf8");
      }
    }
  } finally {
    //
  }
}



module.exports = {
  downloadViaEZProxy,
  normalize,
  download
}
