const sandbox = require("../../sandbox");
const libraryIds = ['g2829873','u39226']; // todo

class CouchbaseMatcher {

  /**
   * Given a query, return the resulting rows, retrying if Couchbase server is misbehaving
   * @param {string} query
   * @returns {Promise<Object[]>}
   * @private
   */
  static async __query(query) {
    let rows;
    let attempts = 3;
    while (attempts > 0) {
      try {
        let cluster = await sandbox.getRemoteCluster();
        ({rows} = await cluster.query(query.replace(/ +/g, " ")));
        return rows;
      } catch (e) {
        if (--attempts === 0) {
          throw e;
        }
        console.error(`Getting "${e.message}" from couchbase server, retrying ${attempts} times ... `);
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
  }


  /**
   * Matches by (segmented) reference string and returns complete Zotero-formatted data or null if no
   * match could be found
   * @param {string} refstring
   * @param {{lastNames:[], year:number, titleWords:[]}} refseg
   * @returns {Promise<Object|0>}
   */
  static async match(refstring, refseg){
    let {lastNames, year, titleWords} = refseg;
    for (let libraryId of libraryIds) {
      let collectionPath = `zotero.${libraryId}.items`;
      let query = `select g.*
                  from ${collectionPath} g
                  where SEARCH(g, 'creators.lastName:"${lastNames[0]}"') 
                    and SEARCH(g, 'title:"${titleWords[0]}"') 
                    and SEARCH(g, 'date:"${year}"')`;
      let rows = await this.__query(query);
      if (rows.length) {
        return rows[0];
      }
    }
    return null;
  }


  /**
   * looks up by specific Zotero item fields and returns completed Zotero-formatted item data or null if not found
   * @param item
   * @returns {Promise<Object>}
   */
  static async lookup(item){
    if (!item.DOI) {
      throw new Error("Item data must contain DOI field");
    }
    // get metadata from Couchbase
    let query = `select g.*
                  from zotero.g2829873.items g
                  where g.DOI = "${item.DOI}"`;
    let rows = await this.__query(query);
    if (!rows.length) {
      return null;
    }
    return rows[0];
  }
}

module.exports = CouchbaseMatcher;
