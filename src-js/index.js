const process = require('process');
const sandbox = require('./sandbox');
const path = require('path');
const fsp = require('fs').promises;

const cmds = {
  CREATE_LOCAL_BUCKETS: "Create all needed buckets locally",
  CREATE_REMOTE_BUCKETS: "Create all needed buckets remotely",
  IMPORT_JATS: "Import de Gruyter JATS data",
  IMPORT_DIGIZEIT: "Harvest Digizeitschriften OAI Data",
  NORMALIZE_ARTICLES: "Normalize article data into Zotero format & save to zotero group",
  NORMALIZE_AUTHORS: "Import & normalize author data from article data",
  DOWNLOAD_FULLTEXTS: "Download article fulltexts from deGruyter and digizeitschriften.de via EZProxy",
  SYNC_COUCHBASE: "Synchronize local couchbase DB with remote",
  SYNC_ZOTERO: "Synchronize zotero data to local copy",
  SYNC_FULLTEXTS: "Synchronize fulltexts stored on zotero and on open semantic search server",
  FIND_MISSING_PDFS: "Search Zotero for missing PDF attachments and try to locate them in the publishers repositories",
  EXPORT_JATS_ZFRSOZ: "Export local zotero data as JATS XML in ZfRsoz citation style",
  OCR_TAGGED_ATTACHMENTS: "Send attachments not yet tagged with 'ocr:abbyy' to OCR service and upload extracted text to Zotero and WebDAV",
  MATCH_EXCITE: "Match the entries in the EXCITE-JSON folder on WebDAV with various data sources and create citnetexplorer file",
  ANALYZE_WOS_FILE: "Analyze the given WOS file",
  HANDLE_UNIDENTIFIED: "Handle the entries in the 'Cited Unidentified' collection",
  KBFIZ_TO_WOS: "Create a WOS export file from KB fiz-karlsruhe CSV exports"
};

async function existingFile(filepath) {
  if (! (await fsp.stat(filepath)).isFile()) {
    throw new Error(`File does not exist: ${filepath}`);
  }
  return filepath;
}

// main
(async () => {
  try {
    let cli = {};
    for (let key of Object.keys(cmds)) {
      let cliCmd = key.toLowerCase().replace(/_/g,"-");
      cli[cliCmd] = key;
    }
    let cmd = process.argv[2];
    let cluster;
    switch(cmds[cli[cmd]]){
      case cmds.CREATE_LOCAL_BUCKETS:
        cluster = await sandbox.getLocalCluster();
      case cmds.CREATE_REMOTE_BUCKETS:
        cluster = cluster || await sandbox.getRemoteCluster();
        for (let bucketName of Object.values(sandbox.buckets)) {
          await sandbox.createBucket(cluster, bucketName);
        }
        break;
      case cmds.SYNC_COUCHBASE:
        await require("./sync").syncAllBuckets();
        break;
      case cmds.IMPORT_JATS:
        await require("./import/deGruyterJats").importData();
        break;
      case cmds.IMPORT_DIGIZEIT:
        await require("./import/digiZeitschriften").importData(true);
        break;
      case cmds.NORMALIZE_AUTHORS:
        await require("./normalize/authors").normalize();
        break;
      case cmds.NORMALIZE_ARTICLES:
        await require("./normalize/articles").normalize();
        break;
      case cmds.DOWNLOAD_FULLTEXTS:
        await require("./normalize/articles").download();
        break;
      case cmds.SYNC_FULLTEXTS:
        await require('./sync').syncFulltexts();
        break;
      case cmds.FIND_MISSING_PDFS:
        await require('./sync').findMissingPdfs();
        break;
      case cmds.SYNC_ZOTERO:
        await require('./sync').zoteroSync();
        break;
      case cmds.EXPORT_JATS_ZFRSOZ:
        await require('./export').jats_zfrsoz();
        break;
      case cmds.OCR_TAGGED_ATTACHMENTS:
        const libraryId = `g${process.env.ZOTERO_GROUP}`;
        const options = {
          filter: {date:"19%"},
          findTag: "!ocr:abbyy",
          applyTag: "ocr:abbyy",
          update: true,
          language: "German",
          exportFormat: "txtUnstructured,pdfa,docx",
          extension: "txt,pdf,docx",
          outDir: path.join(__dirname, "..", "out"),
          progressFunc: sandbox.gauge.show.bind(sandbox.gauge),
          errorFunc: err => console.error("\n" + err),
          deleteAfterUpload: false,
          renameToDoi: true,
          useRemote: true,
          abbyyCustomOptions: "txtUnstructured:paragraphAsOneLine=true",
          debug:false,
          uploadToZotero:true,
          skipIfExistsOnZotero:true
        };
        await require('./ocr').ocr(libraryId, options);
        break;
      case cmds.MATCH_EXCITE: {
        let outfile;
        if (process.argv[3]) {
          outfile = await existingFile(process.argv[3]);
          console.info(`Using ${outfile} ...`);
        } else {
          outfile = `out/wos-${new Date().toISOString().replace(/[^\d]/g, "-").substr(0,19)}.txt`
          console.info(`Creating ${outfile}`);
        }
        const dir = "/EXCITE-JSON/";
        const encoding = "latin1";
        await require('./match/index').matchExcite(dir, outfile, encoding);
        break;
      }
      case cmds.ANALYZE_WOS_FILE: {
        const outfile = await existingFile(process.argv[3]);
        console.log(await require('./match/index').analyzeWosFile(outfile));
        break;
      }
      case cmds.HANDLE_UNIDENTIFIED: {
        console.log(await require('./match/index').handleUnidentified());
        break;
      }
      case cmds.KBFIZ_TO_WOS: {
        await require('./match/matcher/kbfiz').createWosFileFromCsv();
        break;
      }
      default:
        console.info(cmd? `Unknown command "${cmd}".`: "Usage:");
        console.info(
          Object.keys(cmds)
            .map(key => `${key.toLowerCase().replace(/_/g,"-").padEnd(30)}${cmds[key]}`)
            .join("\n")
        );
    }
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
  process.exit(0);
})();
