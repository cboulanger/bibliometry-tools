const sandbox = require("./sandbox");
const process = require('process');
const fs = require('fs');
const fsp = require('fs').promises;
const os = require('os');
const path = require('path');

/**
 * Very simple one-way sync local -> remote or remote -> local depending on where more records are,
 * it is assumed that more records === newer. FIXME better sync algorithm needed!
 * @param bucketName
 * @returns {Promise<void>}
 */
async function syncBucket(bucketName){
  const localCluster = await sandbox.getLocalCluster();
  const localBucket = localCluster.bucket(bucketName).defaultCollection();
  const remoteCluster = await sandbox.getRemoteCluster();
  const remoteBucket = remoteCluster.bucket(bucketName).defaultCollection();
  const localIds = (await localCluster.query("select raw meta().id from `" + bucketName + "`;")).rows;
  const remoteIds = (await remoteCluster.query("select raw meta().id from `" + bucketName + "`;")).rows;

  let counter = 0;
  let total;

  if ( localIds.length > remoteIds.length ) {
    total = localIds.length;
    for (let id of localIds) {
      counter++;
      sandbox.gauge.show(`Synchronizing ${bucketName} local -> remote ${counter}/${total} `, counter/total);
      let data = (await localBucket.get(id)).content;
      await remoteBucket.upsert(id, data);
    }
  } else if ( remoteIds.length > localIds.length) {
    total = remoteIds.length;
    for (let id of remoteIds) {
      counter++;
      sandbox.gauge.show(`Synchronizing ${bucketName} remote -> local ${counter}/${total} `, counter/total);
      let data = (await remoteBucket.get(id)).content;
      await localBucket.upsert(id, data);
    }
  } else {
    console.log("Up-to-date.");
  }
  sandbox.gauge.hide();
}

/**
 * Sync all named buckets
 * @returns {Promise<void>}
 */
async function syncAllBuckets() {
  for (let bucketName of Object.values(sandbox.buckets)) {
    console.info(`Synchronizing ${bucketName}`);
    await this.syncBucket(bucketName);
  }
}

async function fixAllBuckets() {
  for (let bucketName of Object.values(sandbox.buckets)) {
    console.info(`Fixing ${bucketName}`);
    const localCluster = await sandbox.getLocalCluster();
    const defLocalColl = localCluster.bucket(bucketName).defaultCollection();
    const localIds = (await localCluster.query("select raw meta().id from `" + bucketName + "`;")).rows;
    let counter = 0;
    let total = localIds.length;
    for (let id of localIds){
      counter++;
      sandbox.gauge.show(`Fixing ${bucketName} ${counter}/${total} `, counter/total);
      let data = (await defLocalColl.get(id)).content;
      let fixed = 0;
      while (data && typeof data == "object" && typeof data.content == "object") {
        data = data.content;
        fixed = true;
      }
      if (fixed) {
        await defLocalColl.upsert(id, data);
      }
    }
    sandbox.gauge.hide();
  }
}


/**
 * Sync fulltexts, currently a one-way sync from the fulltext repo -> Zotero
 * @returns {Promise<void>}
 */
async function syncFulltexts() {
  const localCluster = await sandbox.getLocalCluster();
  const localIds = (await localCluster.query("select raw meta().id from articles;")).rows;
  const articlesColl = await sandbox.getBucketDefaultCollection("articles");
  let tmpdir = await fsp.mkdtemp(path.join(os.tmpdir(), 'sync-'));
  let count = 0;
  let total = localIds.length;
  for (let id of localIds) {
    count++;
    let record = (await articlesColl.get(id)).content;
    if (!record.key) {
      console.warn(`Record with id ${id} has no Zotero item key`);
      continue;
    }
    for (let ext of ["pdf","txt"]) {
      let filename = id.replace(/[/:|]/g,"_") + "." + ext;
      try {
        let localPath = path.join(tmpdir, filename);
        let attachments = await sandbox.zoteroApi.getAttachments(record.key, {filename});
        // skip existing attachment for the moment
        if (!attachments.length){
          sandbox.gauge.show(`Uploading to zotero item '${record.title.substr(0,20)}' (${count}/${total})`, count/total);
          await sandbox.zoteroApi.uploadAttachment(record.key, localPath);
        } else {
          sandbox.gauge.show(`Checking zotero item ${count}/${total}`, count/total);
        }
      } catch (e) {
        sandbox.logger.error(filename + ": " + e.message);
      }
    }
  }
}

async function findMissingPdfs() {
  const scissors = require('scissors');
  const getDownloadUrl = require('./import/digiZeitschriften').getDownloadUrl;
  const downloadViaEZProxy = require('./normalize/articles').downloadViaEZProxy;
  const cluster = await sandbox.getLocalCluster();
  const libraryId = `g${process.env.ZOTERO_GROUP}`;
  const collectionPath = `zotero.${libraryId}.items`;
  const where = condition => `select g.* from ${collectionPath} g where ${condition}`;
  const outdir = path.join(process.cwd(), "out");
  // update cache
  await zoteroSync(libraryId);
  // find items which do not have attachments
  const missingIdsQuery = `
    SELECT raw g.DOI
    FROM ${collectionPath} g
    WHERE g.DOI IS NOT MISSING
        AND g.parentKey IS MISSING
        AND g.\`key\` NOT IN (
        SELECT RAW c.parentItem
        FROM ${collectionPath} c
        WHERE c.parentItem IS NOT MISSING)`;
  let missingIds = (await cluster.query(missingIdsQuery)).rows;
  let count = 0;
  let total = missingIds.length;
  for (let id of missingIds) {
    count++;
    sandbox.gauge.show(`Checking ${id} ${count}/${total}`, count / total);
    let rows = (await cluster.query(where(`DOI = '${id}'`))).rows;
    if (rows.length === 0) {
      continue;
    }
    let item = rows[0];
    let filename = id.replace(/[/:|]/g, "_") + ".pdf";
    let localPath = path.join(outdir, filename);
    let attachments = (await cluster.query(where(`parentItem = '${id}'`))).rows;
    if (!attachments.length) {
      let ppn = item.callNumber;
      if (!ppn) {
        // We need PPN to retrieve pdfs from digizeitschriften.de
        continue;
      }
      try {
        sandbox.gauge.hide();
        let publisherUrl = getDownloadUrl(ppn);
        console.log(`No attachment for ${id}`);
        console.log(` - Trying to download attachment from ${publisherUrl}`);
        await downloadViaEZProxy(publisherUrl, localPath);
        let hasDzCoverPage;
        do {
          console.log(" - Checking for coverpage...");
          hasDzCoverPage = await new Promise((resolve, reject) => {
            let content = "";
            // sometimes scissors gets stuck, abort after 5 secs.
            setTimeout(() => resolve(false), 5000);
            scissors(fs.createReadStream(localPath))
              .textStream()
              .on('data', data => content.length > 1000 ? resolve(false)
                : ((content += data) && content.match(/digizeitschriften/i))
                  ? resolve(true) : null)
              .on('end', () => resolve(false))
              .on('error', reject)
          });
          if (hasDzCoverPage) {
            let tmpFile = localPath + ".cut.pdf";
            console.log(" - Removing coverpage...")
            await new Promise(((resolve, reject) => {
              scissors(fs.createReadStream(localPath))
                .range(2)
                .pdfStream()
                .pipe(fs.createWriteStream(tmpFile))
                .on('finish', resolve)
                .on('error', reject);
            }));
            await fsp.copyFile(tmpFile, localPath);
            await fsp.unlink(tmpFile);
          }
        } while (hasDzCoverPage);
        console.log(" - Running OCR ...");
        let pageCount = await scissors(fs.createReadStream(localPath)).getNumPages();
        let page = 0;
        let tmpFile = localPath + ".ocr.pdf";
        let options = {
          cwd: process.cwd(),
          cmd: "pdfsandwich",
          args: ["-lang deu", `-o ${tmpFile}`, localPath],
          log: msg =>
            msg.match(/Processing page/) &&
            sandbox.gauge.show(`Processing page ${++page}/${pageCount}`, page / pageCount)
        }
        sandbox.gauge.hide();
        await sandbox.runCommand(options);
        sandbox.gauge.hide();
        await fsp.copyFile(tmpFile, localPath);
        await fsp.unlink(tmpFile);
        console.log(" - Extracting OCR text");
        let txtFile = localPath.replace(".pdf", ".txt");
        await sandbox.runCommand(process.cwd(), "pdftotext", localPath);
        console.log(` - Uploading to WebDav`);
        await sandbox.uploadToWebDav(localPath);
        await sandbox.uploadToWebDav(txtFile);
        console.log(` - Uploading PDF attachment to Zotero`);
        await sandbox.zoteroApi.uploadAttachment(item.key, localPath);
        console.log(` - Uploading Text attachment to Zotero`);
        await sandbox.zoteroApi.uploadAttachment(item.key, txtFile);
      } catch (e) {
        // error
        sandbox.logger.error(`Error processing ${id}: ${e.message}`);
        continue;
      }
    }
  }
}

module.exports = {
  syncBucket,
  syncAllBuckets,
  syncFulltexts,
  findMissingPdfs
};

