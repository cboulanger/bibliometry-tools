const sandbox = require("../sandbox");
const process = require('process');
const fs = require('fs');
const fsp = require('fs').promises;
const os = require('os');
const path = require('path');

async function jats_zfrsoz(libraryId=false) {
  let outputFile;
  try {
    outputFile = await fs.promises.open(path.join(__dirname, "..", "..", "out", "jats-zfrsoz.xml"), "w");
    const cluster = await sandbox.getLocalCluster();
    let query = "select raw `path` from system:scopes where `bucket`='zotero'";
    const scopes = (await cluster.query(query)).rows;
    let scopeCounter = 0;
    let scopeNumber = scopes.length;
    for (let scope of scopes) {
      scopeCounter++;
      sandbox.gauge.show(`Exporting library ${scopeCounter}/${scopeNumber}`, );
      let ids = (await cluster.query(`select raw meta().id from ${scope}.items i where i.creators is not missing`)).rows;
      let itemCounter = 0;
      let itemNumber = ids.length;
      let batchSize = 100;
      while (itemCounter < itemNumber) {
        let range = ids.slice(itemCounter, itemCounter + batchSize);
        query = `select items.* from ${scope}.items where meta().id in ${JSON.stringify(range)}`
        let items = (await cluster.query(query)).rows;
        for (let item of items) {
          itemCounter++;
          sandbox.gauge.show(`Exporting item  ${itemCounter}/${itemNumber}`, itemCounter/itemNumber);
          outputFile.write(`<key>${JSON.stringify(item,null,2)}</key>\n`);
          break;
        }
        break;
      }
    }
  } catch (e) {
    sandbox.gauge.hide();
    console.error("error at :" + sandbox.gauge._status.section);
    throw e;
  } finally {
    if (outputFile) {
      await outputFile.close();
    }
  }
}

module.exports = {
  jats_zfrsoz
};
