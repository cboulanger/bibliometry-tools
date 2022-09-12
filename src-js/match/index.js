// Requires a Zotero Translation Server listening on port 1969
// https://github.com/zotero/translation-server

const sandbox = require("../sandbox");
const process = require('process');
const fs = require('fs');
const fsp = require('fs').promises;
const {Item, Collection} = require('../zotero/index');
const levenshtein = require('fast-levenshtein');
const assert = require("assert");
const debug = require('debug')('match');
const path = require("path");

// matchers
const crossrefMatcher = require('./matcher/crossref');
const couchbaseMatcher = require('./matcher/couchbase');
const googleScholarMatcher = require('./matcher/google-scholar');
const sruMatcher = require('./matcher/sru');
const zoteroMatcher = require('./matcher/zotero');
const wosMatcher = require('./matcher/wos');

// todo move into .env
const groupLibrarySlug = "groups/2829873";
const groupLibraryId = 'g2829873';

/**
 * Wrapper function to deal with timeouts
 * @param {string} dir Path to the directory containing the files to analyze
 * @param {string} outfile Path to the result file, will be reused if it exists.
 * @param {string} encoding Encoding of the result file
 * @returns {Promise<void>}
 */
async function matchExcite(dir, outfile, encoding) {
  if (!process.env.ZOTERO_TRANSLATION_SERVER_URL) {
    throw new Error("You must set the ZOTERO_TRANSLATION_SERVER_URL environment variable and ensure a translation server is running and accessible.");
  }
  let tries = 3;
  let counter = 0;
  while (counter < tries) {
    try {
      await matchExciteImpl(dir, outfile, encoding);
      return;
    } catch (e) {
      if (!e.message.match(/timeout/i)){
        throw e;
      }
      counter++;
      console.error(e);
    }
  }
  throw new Error(`Aborting after ${tries} timed out requests.`)
}

/**
 * Implementation
 * @param {string} dir Path to the directory containing the files to analyze
 * @param {string} outfile Path to the result file, will be reused if it exists.
 * @param {string} encoding Encoding of the result file
 * @returns {Promise<void>}
 */
async function matchExciteImpl(dir, outfile, encoding) {

  const library = sandbox.getZoteroLibrary(groupLibraryId); // Todo
  const filepaths = (await fsp.readdir(dir, { withFileTypes: true }))
    .filter(dirent => dirent.isFile())
    .map(dirent => path.join(dir, dirent.name));

  /**
   * Saves a reference in Zotero
   * @param {String} collectionPath
   * @param {Object} data
   * @param {String?} itemType If omitted, the item type must be in the data
   * @returns {Promise<Item>}
   */
  async function saveInZotero(collectionPath, data, itemType) {
    if (itemType) {
      let template = await library.server.getTemplate(itemType);
      data = Object.assign(template, data);
    }
    return library.createItemInCollection(collectionPath, data);
  }

  console.time("Duration");

  // crate output file
  if (!fs.existsSync(outfile)) {
    wosMatcher.createFile(outfile, encoding);
  }
  // create a list of DOIs which are contained in this file, which won't be processed again.
  const doiList = (await fsp.readFile(outfile, {encoding:"utf-8"}))
    .split("\n")
    .filter(line => line.startsWith("DI "))
    .map(line => line.substr(3));

  // contains fragments that cause a reference to ignored
  const ignorefile = path.join(__dirname, "ignore.txt");
  const textToIgnore = (await fsp.readFile(ignorefile, {encoding:"utf-8"})).split("\n");

  // go through all of the excite files with the reference data
  let skippedFiles = 0;
  for (let filepath of filepaths) {
    let filename = filepath.replace(dir+"/", "");
    if (filename === "" || filename[0] === ".") {
      continue;
    }
    // check if we have processed this file already
    const DOI = filename.split(".").slice(0,2).join(".").replace("_","/");
    if (doiList.includes(DOI)) {
      skippedFiles++;
      continue;
    }
    if (skippedFiles > 0) {
      console.log(` - Skipped ${skippedFiles} files which were already analyzed.`);
      skippedFiles=0;
    }
    // get zotero item data from couchbase mirror. Todo: get from Zotero
    const zoteroItemData = await couchbaseMatcher.lookup({DOI});
    if (!zoteroItemData) {
      console.log(` - Skipping DOI ${DOI}: no record.`);
      continue;
    }

    if (zoteroItemData.creators.length === 0) {
      console.log(` - Skipping DOI ${DOI}: record contains no author information.`);
      continue;
    }

    console.log(` - Analyzing document with DOI ${DOI} ...`);

    // the citing Zotero item
    const citingItem = library.item(zoteroItemData.key);

    /**
     * Shorthand function to add the citation relation to the Zotero item.
     * @param {string} citedItemKey The key of the cited item
     * @returns {Promise<void>}
     */
    async function addRelationToCitingItem(citedItemKey) {
      await citingItem.fetch();
      await citingItem.addRelation("dc:relation", `http://zotero.org/${groupLibrarySlug}/items/${citedItemKey}`).save();
    }

    // read CSV of cited references as JSON for inclusion in CR field
    let csv = await fsp.readFile(filepath, "utf8");
    let json = "[" + csv.split('\n').filter(line => Boolean(line.trim())).join(",") + "]";
    let exItems = JSON.parse(json);
    let CR = [];

    // loop over all contained references
    for (let exItem of exItems) {

      try {

        // the reference
        const reference = exItem.ref_text_x.toLocaleLowerCase()
        // the reference string in lower case, for search
        const refstring = exItem.ref_text_x.toLocaleLowerCase();
        // the segmented reference, with probabilities
        const ref = exItem.ref_seg_dic;

        // skip all garbage
        if (!(ref.author && ref.title && ref.year)) {
          debug(`Parser could not find any useful data in ${refstring}`);
          continue;
        }

        // skip text that is included in the "ignore.txt" file
        if (textToIgnore.some(text => refstring.includes(text))) {
          debug(`Skipping '${refstring}' because it contains to-be-ignored text.`);
          continue;
        }

        // this removes wrongly included multiple in-text citations
        let countYears = 0;
        for (let match of refstring.matchAll(/[12][9012][0-9]{2}/g)) countYears++;
        if (countYears > 1) {
          debug(`Too many years in ${refstring}`);
          continue;
        }

        // create identification data from messy segmentation data:
        // a list of last names in lower case
        let lastNames = ref.author
          .map(item => item[0].surname)
          .filter(item => Boolean(item))
          .map(name => name.replace(/[^\p{L}-]/gu," ").split(" ")[0].toLocaleLowerCase()) // remove all non-letters
          .filter(name => name.length >= 3);
        if (lastNames.length === 0) {
          debug(`Parser could not find any useful author data in ${refstring}`);
          continue;
        }

        // the title as determined by the parser
        let title = ref.title.map(t => t.value).join(" ");

        // a list of title words in lower case, ordered by their probability
        let titleWords = ref.title
          .slice(0)
          .sort((a,b) => a.score - b.score)
          .map(word => word.value.replace(/[^\p{L}]/gu,"").toLocaleLowerCase())
          .filter(word => word.length > 3)
        if (titleWords.length === 0) {
          debug(`Parser could not find any useful title data in ${refstring}`);
          continue;
        }

        // the publication year
        let year;
        try {
          year = Number(ref.year[0].value.match(/[12][9012][0-9]{2}/)[0]);
        } catch (e) {}
        if (!year) {
          debug(`Parser could not find any publication year data in ${refstring}`);
          continue;
        }

        // the segmentation data passed to the matchers
        const refseg = {
          lastNames,
          year,
          titleWords,
          title
        }

        console.log(` - Looking up ${reference}`)

        // look in Zotero data mirrored in couchbase, todo: look in Zotero directly
        {
          let cited = await zoteroMatcher.match(refstring, refseg);
          if (cited) {
            if (!cited.key) {
              // we don't have a key, this means it's copied from a different library
              let collectionPath;
              switch (cited.itemType) {
                case "journalArticle":
                  collectionPath = `Cited Journals/${cited.publicationTitle}`;
                  break;
                case "bookSection":
                  let creatorStr = cited.creators.slice(0,3).map(c => c.lastName).join("; ");
                  let folderTitle = `${creatorStr} (${cited.date}) ${cited.bookTitle.substr(0, 20)}`
                  collectionPath = `Cited Books/${folderTitle}`;
                  break;
                default:
                  collectionPath = `Cited Books`;
              }
              cited = await library.createItemInCollection(collectionPath, cited);
              console.log(`   - Created ${lastNames[0]} (${year}) in ${collectionPath}.`);
            }
            await addRelationToCitingItem(cited.key);
            CR.push(wosMatcher.createCrFieldEntry(cited));
            console.log(`   √ Found ${lastNames[0]} (${year}) in Zotero.`);
            continue;
          }
        }

        // is it a journal?
        let probably_a_journal = Boolean(ref.source) ||
          ["journal", "zeitschrift", "review"].some(jt => refstring.includes(jt));
        if (probably_a_journal) {
          // lookup in crossref
          let cited = await crossrefMatcher.match(refstring, refseg);
          let citedItem;
          let collectionPath;
          if (cited) {
            switch (cited.itemType) {
              case "book":
                collectionPath = `Cited Books/${cited.publicationTitle}`;
                break;
              case "bookSection":
                let creatorStr = lastNames.join("; ");
                let folderTitle = `${creatorStr} (${cited.date}) ${cited.bookTitle.substr(0, 20)}`
                collectionPath = `Cited Books/${folderTitle}`;
                break;
              case "journalArticle":
                collectionPath = `Cited Journals/${cited.publicationTitle}`;
                break;
              default:
                collectionPath = `Cited Unidentified`;
            }
            citedItem = await saveInZotero(collectionPath, cited);
            await addRelationToCitingItem(citedItem.key);
            CR.push(wosMatcher.createCrFieldEntry(cited));
            console.log(`   √ Found ${lastNames[0]} (${year}) in CrossRef and saved in ${collectionPath}.`);
            continue;
          } else {
            //console.log(`   x ${lastNames[0]} (${year}) not found in Crossref.`);
          }
        } else {
          // probably a book, lookup in SRU-Servers
          let cited = await sruMatcher.match(refstring, refseg);
          if (cited) {
            let citedItem = await saveInZotero(`Cited Books`, cited);
            await addRelationToCitingItem(citedItem.key);
            CR.push(wosMatcher.createCrFieldEntry(cited));
            console.log(`   √ Found  ${lastNames[0]} (${year}) via SRU.`);
            continue;
          } else {
            //console.log(`   x ${lastNames[0]} (${year}) not found in SRU servers.`);
          }
        }
        /*
              if (!this.too_many_gs_requests) {
                try {
                  let cited = await googleScholarMatcher.match(refstring, refseg);
                  if (cited) {
                    if (!cited.itemType) {
                      // zotero data returned by the GS matcher might be incomplete
                      cited = Object.assign(await library.server.getTemplate("journalArticle"), cited);
                    }
                    let citedItem = await saveInZotero(`Cited Unidentified`, cited);
                    await addRelationToCitingItem(citedItem.key);
                    CR.push(wosMatcher.createCrFieldEntry(cited));
                    console.log(`   √ Found  ${lastNames[0]} (${year}) via Google Scholar.`);
                    continue;
                  } else {
                    //console.log(`   x ${lastNames[0]} (${year}) not found in Google Scholar.`);
                  }
                } catch (e) {
                  if (e.message.includes("429")) {
                    this.too_many_gs_requests = true;
                    console.error("   - Blocked by Google Scholar because of too many requests.");
                  } else {
                    throw e;
                  }
                }
              }
        */
        // reference could not be identified, create an incomplete entry
        let cited = { creators:[] };
        let itemType = "book";
        if (ref.source) {
          if (ref.editor) {
            itemType = "bookSection"
            cited.creators = cited.creators.concat({creatorType: "editor", name: ref.editor.map(e=>e.value).join(" ")});
            cited.bookTitle = ref.source.map(s => s.value).join(" ");
          } else {
            itemType = "journalArticle";
            cited.publicationTitle = ref.source.map(s => s.value).join(" ");
            if (ref.volume) {
              cited.volume = ref.volume[0].value;
            }
            if (ref.pages){
              cited.pages = ref.pages[0].value;
            }
          }
        }
        cited.creators = cited.creators.concat(ref.author
          .map(a => a[0])
          .reduce((prev, curr) => {
            if (curr.surname) {
              prev.push({ creatorType: "author", lastName: curr.surname, firstName: ""})
            } else if (curr["given-names"]) {
              prev[prev.length-1].firstName = curr["given-names"];
            }
            return prev;
          }, []));

        cited.title = title;
        cited.date = year;
        cited.abstractNote = reference;
        cited = Object.assign(await library.server.getTemplate(itemType), cited);
        let citedItem = await saveInZotero(`Cited Unidentified`, cited);
        await addRelationToCitingItem(citedItem.key);
        CR.push(wosMatcher.createCrFieldEntry(cited));
        console.log(`   - Added unidentified citation '${lastNames[0]} (${year})'.`);
      } catch (e) {
        // ignore error so that script can run to completion
        console.error(e.message);
      }
    }

    //
    // wrap-up
    //

    if (CR.length) {
      console.log(`- Found ${CR.length} citations.`);
    } else {
      console.log("- No citations were found.")
    }
    await wosMatcher.appendRecord(outfile, zoteroItemData, CR, encoding)
    debug("Wrote record to file");
    // todo mark as matched
    //await citingItem.fetch();
    //await citingItem.addTag(``)
    console.log("-".repeat(80));
  }
  console.timeEnd("Duration");
}

async function analyzeWosFile(outfile, dataOnly=false) {
  const lines = (await fsp.readFile(outfile, {encoding:"utf-8"})).split("\n");
  let records = 0;
  let citations = 0;
  let identified = 0;
  let inCR = false
  for (let line of lines) {
    if (inCR) {
      if (line.startsWith("   ")) {
        citations++;
        if (line.match(/ISBN |DOI /)) identified++;
        continue;
      }
      inCR = false;
    } else if (line.startsWith("PT ")) {
      records++;
    } else if (line.startsWith("CR ")) {
      inCR = true;
      citations++;
      if (line.match(/ISBN |DOI /)) identified++;
    }
  }
  let recognition_rate = Math.round(identified/citations * 10000)/100;
  if (dataOnly) {
    return {records, citations, identified, recognition_rate};
  }
  return `Analyzed ${records} documents and found ${citations} citations, ${identified} of which (${recognition_rate}%) having a unique identifier (DOI/ISBN)`;
}

async function handleUnidentified() {
  const library = sandbox.getZoteroLibrary(groupLibraryId);
  const collUnidentified = await Collection.byPath(library, "Cited Unidentified");
  console.log( `${await collUnidentified.size()} unidentified citations`);
  let journal = "Zeitschrift für Rechtssoziologie";
  let collectionPath = `Cited Journals/${journal}`;
  let targetCollection = await Collection.byPath(library, collectionPath);
  assert.notEqual(targetCollection, false);
  for await (let item of collUnidentified.items()) {
    if (item.get("itemType") === "journalArticle") {
      let pubTitle = item.get("publicationTitle");
      if (levenshtein.get(journal, pubTitle) < 5) {
        console.log(` - Moving ${item.get("title")} to '${collectionPath}'`);
        await item.copyTo(targetCollection);
        await item.removeFrom(collUnidentified);
      }
    }
  }
}

module.exports = {
  matchExcite,
  analyzeWosFile,
  handleUnidentified
};
