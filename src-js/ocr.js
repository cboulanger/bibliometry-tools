const sandbox = require("./sandbox");
const process = require('process');
const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');
const {AbbyyOcr, ProcessingSettings} = require('@cboulanger/abbyy-cloud-ocr');

/**
 * OCRs the PDF attachments of (optionally filtered) items in the given group,
 * using the Abbyy OCR web service, and uploads the ocr'd pdfs back to Zotero
 * and optionally to a Webdav server. If the deleteAfterUpload option is false,
 * returns an array of file paths to the generated files, otherwise returns an empty
 * array.
 *
 * @param {String} libraryId The library id, preceeded by "g" for groups and "u" for user libraries.
 * @param {Object} options A map of optional configuration values:
 *  -  {String} findTag Process items that have this tag. If the tag is preceded by a "!",
 * only Process all item that do not have this tag
 *  - {String} applyTag After successful OCR and text extraction, apply this tag to the PDF attachment
 *  - {Boolean} update Whether to update the local data from the Zotero server first
 *  - {String} language, see ..., defaults to "English"
 *  - {String} exportFormat, see ..., defaults to "pdfa"
 *  - {String} extension, the extension the generated file should be saved with. Defaults to ".pdf"
 *  - {Object} filter A map of item fields and values that is used to filter the set of *parent items*
 *   the pdf attachements of which are processed. Works only for string fields, N1QL wildcards can be used.
 *  - {String} outDir The directory in which the files should be saved. Defaults to the OS tmp dir
 *  - {Boolean} deleteAfterUpload Whether to delete the generated files after the upload
 *  - {Function} reportFunc A function that reports the progress back to the user. Defaults to console.log
 *  - {Function} errorFunc A function that reports an error message to the user (without throwing). Defaults to console.error
 *
 * @returns {Promise<{String[]}>}
 */
async function ocr(libraryId, options){
  // config
  let {
    findTag,
    applyTag,
    language = "English",
    exportFormat = "pdfa",
    extension = "pdf",
    filter,
    outDir = require('os').tmpdir(),
    deleteAfterUpload = true,
    progressFunc = console.log,
    errorFunc = console.error,
    renameToDoi = false,
    useRemote = false,
    abbyyCustomOptions = "",
    debug = false,
    uploadToWebdDav = true,
    webDavFolder = "_NEU",
    uploadToZotero = true,
    skipIfExistsOnZotero = true
  } = options;

  // libraries
  const abbyyClient = sandbox.createAbbyyClient();
  const abbySettings = new ProcessingSettings(language, exportFormat, abbyyCustomOptions);
  const cluster = useRemote ? await sandbox.getRemoteCluster() : await sandbox.getLocalCluster();
  const collectionPath = `zotero.${libraryId}.items`;
  const library = sandbox.getZoteroLibrary(libraryId);
  let condition = `g.contentType = "application/pdf"`;
  // find attachments (not) matching the tag
  if (findTag) {
    let operator = {
      condition: "any",
      comparison: "="
    };
    if (findTag[0] === "!") {
      operator = {
        condition: "every",
        comparison: "!="
      };
      findTag = findTag.slice(1);
    }
    condition += ` and ${operator.condition} t in g.tags satisfies t.tag ${operator.comparison} "${findTag}" end`;
  }
  // filter
  if (filter && typeof filter == "object") {
    let parentConditions = Object.entries(filter)
      .map(([key, value]) => `p.${key} like "${value}"`);
    condition += ` and g.parentItem IN (
        select raw p.\`key\`
        from ${collectionPath} p
        where ${parentConditions.join(" and ")})`;
  }
  let query = `select g.* from ${collectionPath} g where ${condition}`;
  let items = (await cluster.query(query)).rows;
  let total = items.length;
  if (total === 0) {
    console.log("No attachments found");
    return;
  }
  let filePaths = [];
  let count = 0;
  let filename;
  if (debug) {
    abbyyClient.emitter.on(AbbyyOcr.event.uploading, filename => console.log(`\n >>> Uploading ${filename}`));
    abbyyClient.emitter.on(AbbyyOcr.event.processing, filename => console.log(`\n >>> Processing ${filename}`));
    abbyyClient.emitter.on(AbbyyOcr.event.downloading, filename => console.log(`\n >>> Downloading ${filename}`));
  }
  if (debug) {
    console.log(items.map(item => item.filename));
  }
  outer_loop:
    for (let item of items) {
      const itemObj = library.item(item.key);
      count++;
      let report = msg => progressFunc(`(${count}/${total}) ${msg}`, count / total);
      try {
        filename = item.filename;
        let parentItem;
        if (renameToDoi) {
          // rename to the DOI, if it has one
          parentItem = await itemObj.parentItem();
          if (parentItem) {
            await parentItem.fetch();
            let doi = parentItem.get("DOI");
            if (doi.includes("10.")) {
              filename = doi.replace('/',"_") + ".pdf";
            }
          }
        }
        if (skipIfExistsOnZotero) {
          // skip this record if the parent record has a child with that filename already
          parentItem = parentItem || await itemObj.parentItem();
          if (parentItem) {
            for (let child of await parentItem.children()) {
              if (child.get("filename").includes(filename)) {
                report(`${filename} already uploaded to Zotero.`);
                await (await itemObj.fetch()).addTag("ocr:abbyy").save();
                continue outer_loop;
              }
            }
          }
        }
        if (filename !== item.filename) {
          report(`Downloading ${item.filename} as ${filename}...`);
        } else {
          report(`Downloading ${item.filename} ...`);
        }

        const attachment = sandbox.getZoteroLibrary(libraryId).attachment(item.key);
        let url = await attachment.downloadUrl();
        let attachmentPath = path.join(outDir, filename);
        await sandbox.download(url, attachmentPath);
        let abbyInfo = await abbyyClient.getApplicationInfo();
        report(`OCR'ing (${abbyInfo.pages} pages left)... `);
        if (abbyInfo.pages === 0) {
          errorFunc("No more Abbyy credits. Exiting.");
          process.exit(1);
        }
        await abbyyClient.process(attachmentPath, abbySettings);
        for await (const filePath of abbyyClient.downloadResult(outDir)) {
          filename = path.basename(filePath);
          try {
            if (uploadToWebdDav) {
              report(`Uploading ${filename} to WebDAV ...`);
              await sandbox.uploadToWebDav(filePath, webDavFolder + "/" + filename);
              await new Promise(resolve => setTimeout(resolve, 1000)); // debug
            }
            if (uploadToZotero) {
              report(`Uploading ${filename} to Zotero ...`);
              const options = {
                handleDuplicateFilename: "replace"
              };
              if (findTag) {
                options.tags = [applyTag];
              }
              await attachment.upload(filePath, options);
              await (await itemObj.fetch()).addTag("ocr:abbyy").save();
            }
            if (deleteAfterUpload && (uploadToZotero || uploadToWebdDav)) {
              await fs.promises.unlink(filePath);
            } else {
              filePaths.push(filePath);
            }
          } catch (e) {
            errorFunc(`Error downloading/uploading ${filename}: ${e.message}`);
            continue;
          }
        }
      } catch (e) {
        throw e;
        //errorFunc(`Error processing ${item.title}: ${e.message}`);
        //continue;
      }
    }
  // done!
  return filePaths;
}

module.exports = {
  ocr
};
