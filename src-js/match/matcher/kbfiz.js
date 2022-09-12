const parse = require('csv-parse/lib/sync')
const fsp = require('fs/promises');
const path = require('path');
const crossref = require('./crossref');
const wos = require('./wos');
const sandbox = require('../../sandbox');
const process = require('process');
const debug = require('debug')('kbfiz');

const dataDir = "data/metadata/kbfiz";
const itemCacheFile = path.join(dataDir, "item_cache.json");

class Kbfiz {

  static async createWosFileFromCsv() {
    const options = {
      columns: true,
      skip_empty_lines: true
    };
    const fileContents = [];
    for (let type of ["AUTHORS_PUBLICATIONS", "ITEMS_CITED", "REFS"]) {
      fileContents.push(parse(await fsp.readFile(`${dataDir}/${type}.csv`, "utf8"), options));
    }
    const [authorPublications, itemsCited, refs] = fileContents;

    // create lookup table for cited items
    const citedItemsLookup = itemsCited.reduce((map, item) => {
      if ("UT_EID" in item) {
        map[item.UT_EID] = item;
      }
      return map;
    }, {});

    // cache items to avoid crossref lookups
    const itemCache = (await fsp.stat(itemCacheFile)).isFile() ? JSON.parse(await fsp.readFile(itemCacheFile, "utf8")) : {};
    let saveCounter = 0;
    async function saveCache(force=false) {
      // only save every 10 calls to this function, unless forced
      if (force || ++saveCounter > 10) {
        await fsp.writeFile(itemCacheFile, JSON.stringify(itemCache, null, 2), "utf8");
        debug("Flushed cache to disk.");
        saveCounter = 0;
      }
    }

    //  crossref lookup
    async function lookupByDoi(DOI) {
      let tries = 3;
      do {
        try {
          return await crossref.lookup({DOI});
        } catch (e) {
          if (!e.message.includes("Not Implemented")) {
            throw e;
          }
          debug(`Translation server reported an error, retrying ${tries} times...`);
          await new Promise(resolve => setTimeout(resolve, 2000));
        }
      } while (--tries > 0);
      throw new Error("Error requesting record from translation server");
    }

    // iterate over all references to retrieve citing and cited item
    const citing2Cited = {};
    let counter = 0;
    let total = refs.length;
    for (let ref of refs) {
      let DOI = ref.DOI;
      if (!DOI) {
        continue;
      }
      sandbox.gauge.show(`Processed ${counter} of ${total} references: ${DOI}`, counter/total);
      // get citing item data
      let citingItem;
      if (DOI in itemCache) {
        // cached
        citingItem = itemCache[DOI];
        if (citingItem === null) {
          continue;
        }
      } else {
        // from crossref
        try {
          citingItem = await lookupByDoi(DOI);
          itemCache[DOI] = citingItem;
          debug(`Updated cache with crossref data for citing item ${DOI}`)
        } catch (e) {
          debug(`!! Unable to get crossref data for citing item ${DOI}`);
          itemCache[DOI] = null;
          continue;
        }
        await saveCache();
      }
      if (!ref.WOS_UID) {
        debug("!! Skipping reference without WOS_UID");
        continue;
      }
      if (!(ref.WOS_UID in citedItemsLookup)) {
        debug(`!! Skipping reference with non-existing item data: ${ref.WOS_UID}` );
        continue;
      }
      // get cited item data
      let citedItem = citedItemsLookup[ref.WOS_UID];
      const citedDOI = citedItem.DOI;
      if (citedDOI) {
        if (citedDOI in itemCache) {
          // cached
          citedItem = itemCache[citedDOI];
          if (citedItem === null) {
            continue;
          }
        } else {
          // get from crossref
          try {
            citedItem = await lookupByDoi(citedDOI);
            itemCache[citedDOI] = citedItem;
            await saveCache();
            debug(`Updated cache with crossref data for cited item ${citedDOI}`);
          } catch (e) {
            debug(`!! Unable to get crossref data for cited item ${citedDOI}`);
            citedItem.DOI = null;
          }
        }
      }
      if (!citedItem.DOI) {
        if (!citedItem.ARTICLE_TITLE) {
          continue;
        }
        citedItem = {
          itemType: "journalArticle",
          creators: authorPublications
            .filter(pub => pub.UT_EID === ref.WOS_UID)
            .sort((a,b) => a.AUTHOR_POSITION - b.AUTHOR_POSITION)
            .map(pub => ({
              lastName: pub.LASTNAME,
              firstName: pub.FIRSTNAME,
              creatorType: "author"
            })),
          title: citedItem.ARTICLE_TITLE.toLocaleLowerCase(),
          date: citedItem.PUBYEAR,
          publicationTitle: (citedItem.SOURCETITLE || "").toLocaleLowerCase(),
          volume: citedItem.VOLUME,
          issue: citedItem.ISSUE,
          pages: citedItem.FIRSTPAGE + " - " + citedItem.LASTPAGE
        };
        if (citedDOI) {
          itemCache[citedDOI] = citedItem;
          await saveCache();
        }
      }
      if (!(DOI in citing2Cited)) {
        citing2Cited[DOI] = [];
      }
      citing2Cited[DOI].push(citedItem);
      counter++;
    }
    await saveCache(true);

    console.log("Generating WOS/ISI file...");
    const encoding = "latin1";
    const outfile = path.join(dataDir, "wos.txt")
    await wos.createFile(outfile)
    for (let [DOI, refs] of Object.entries(citing2Cited)) {
      await wos.appendRecord(outfile, itemCache[DOI], refs.map(ref => wos.createCrFieldEntry(ref)), encoding);
    }

    console.log("Done.");
  }
}


module.exports = Kbfiz;
