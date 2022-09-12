const libraryIds = ['g2829873','u39226']; //todo
const {Server} = require("../../zotero");
const process = require("process");
const inquirer = require("inquirer");
const server = new Server(process.env.ZOTERO_API_KEY);
const debug = require('debug')('zotero-matcher');
const debugVerbose = require('debug')('zotero-matcher-verbose');
const levenshtein = require('fast-levenshtein');
const levDistMax = 10; // todo generalize this

class ZoteroMatcher {

  /**
   * Matches by (segmented) reference string and returns complete Zotero-formatted data or null if no
   * match could be found
   * @param {string} refstring
   * @param {{lastNames:[], year:number, titleWords:[]}} refseg
   * @returns {Promise<Object|null>}
   */
  static async match(refstring, refseg){
    let {lastNames, year, titleWords, title} = refseg;
    for (let libraryId of libraryIds) {
      let query = `${lastNames[0]} ${year}`;
      debug(`Querying Zotero with "${query}"`);
      let library = server.library(libraryId);
      let items = await library.search(query);
      debug(`Got ${items.length} results.`);
      let choices = [];
      for (let item of items) {
        let data = await item.data();
        // at least one title word must match (very low bar)
        if (!titleWords.some(word => data.title.toLocaleLowerCase().includes(word))) {
          debugVerbose({reason: "no title words match", title: data.title, titleWords})
          continue;
        }
        // use the shorter part for Levemshtein distance analysis
        let l = Math.min(title.length, data.title.length);
        if (levenshtein.get(title.substr(0,l), data.title.substr(0,l)) > levDistMax) {
          debugVerbose({reason: "levenshtein distance in title to large", title: data.title, titleWords})
          continue;
        }
        if (libraryId !== libraryIds[0]) {
          // mark for copying into main library
          data.key = null;
        }
        let authors = data.creators.map(au => au.name ? au.name : `${au.lastName}, ${au.firstName}`).join(";");
        let name = `${authors} (${data.date}), ${data.title}`;
        choices.push({
          name,
          value: data
        });
      }
      switch (choices.length) {
        case 0:
          return null;
        case 1:
          return  choices[0].value;
        default: {
          if (Number(process.env.MATCH_NON_INTERACTIVE)) {
            debug({reason: "multiple choices in non-interactive mode, using first match", choices});
            return choices[0].value;
          }
          choices.unshift({
            name: "--- no match ---",
            value: null
          });
          let {data} = await inquirer.prompt([{
            type: "list",
            name: "data",
            message: "Select the matching library reference:",
            choices
          }]);
          return data;
        }
      }
    }
  }
}

module.exports = ZoteroMatcher;
