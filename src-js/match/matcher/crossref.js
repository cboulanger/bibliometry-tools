const fetch = require("node-fetch");
const inquirer = require("inquirer");
const process = require('process');
const levenshtein = require("fast-levenshtein");
const debug = require('debug')('crossref');
const debugVerbose = require('debug')('crossref-verbose');

const levDistMax = 10; // todo generalize this

/**
 * Crossref lookup (DOI-centered)
 */
class CrossrefMatcher {

  /**
   * Matches by (segmented) reference string and returns complete Zotero-formatted data or null if no
   * match could be found
   * @param {string} refstring
   * @param {{lastNames:[], year:number, titleWords:[]}} refseg
   * @returns {Promise<Object|0>}
   */
  static async match(refstring, refseg ){
    let {lastNames, year, titleWords, title} = refseg;
    let result = await fetch(`https://api.crossref.org/works?query=${encodeURIComponent(refstring)}`);
    if (!result.ok) {
      throw new Error(result.statusText)
    }
    // example: https://api.crossref.org/works/10.1111/j.0956-7976.2004.00694.x
    result = await result.json();
    let items = result.message.items;
    let choices = [];
    for (let item of items) {
      try {
        // year must match
        let yp = ["published-print", "published", "published-online", "issued"]
          .reduce((yp, prop) => yp ? yp : item[prop], null);
        if (!(yp &&  typeof yp == "object" && Array.isArray(yp["date-parts"])  )) {
          debugVerbose({reason: "no year information", item})
          continue;
        }
        yp = Number(yp["date-parts"][0][0]);
        if (yp !== year) {
          debugVerbose({reason: "year does not match", yp, year})
          continue;
        }
        // at least one author must match
        if (!item.author) {
          debugVerbose("no author!")
          continue;
        }
        let au = item.author.map(a => `${a.given} ${a.family}`).join(", ");
        if (!lastNames.some(name => au.toLocaleLowerCase().includes(name))) {
          debugVerbose({reason: "authors do not match", au, lastNames})
          continue;
        }

        // at least one title word must match
        // let ti = item.title[0];
        // if (!titleWords.some(word => ti.toLocaleLowerCase().includes(word))) {
        //   debugVerbose({reason: "title words do not match", ti, titleWords})
        //   continue;
        // }

        // use the shorter part for Levemshtein distance analysis
        let l = Math.min(title.length, ti.length);
        if (levenshtein.get(title.substr(0,l), ti.substr(0,l)) > levDistMax) {
          debugVerbose({reason: "levenshtein distance in titles to large", ti, title});
          continue;
        }
        let ct = (item["short-container-title"] || item["container-title"] || [""] )[0];
        let name = `${au} (${yp}), ${ti}, ${ct}`;
        choices.push({
          name,
          value: item.DOI
        });
      } catch (e) {
        console.error(e.message);
        debug({reason: "item crashes checks", error:e, item});
      }
    }
    debug(`Using ${choices.length} of ${items.length} results`);
    let DOI;
    switch(choices.length) {
      case 0:
        return null;
      case 1:
        DOI = choices[0].value;
        break;
      default: {
        if (Number(process.env.MATCH_NON_INTERACTIVE)) {
          debug({reason: "multiple choices in non-interactive mode, no match", choices});
          return null;
        }
        choices.unshift({
          name: "--- no match ---",
          value: null
        });
        ({DOI} = await inquirer.prompt([{
          type: "list",
          name: "DOI",
          message: "Select the matching CrossRef reference:",
          choices
        }]));
        if (!DOI) {
          return null;
        }
      }
    }
    return this.lookup({DOI})
  }

  /**
   * Looks up by specific Zotero item fields and returns completed Zotero-formatted item data
   * @param item
   * @returns {Promise<Object>}
   */
  static async lookup(item){
    if (!item.DOI) {
      throw new Error("Item data must contain DOI field");
    }
    // use translation server to query data from crossref
    let response = await fetch(`${process.env.ZOTERO_TRANSLATION_SERVER_URL}/search`, {
      method:"POST",
      headers: {"Content-Type":"text/plain"},
      body: item.DOI
    });
    if (response.ok) {
      let data = await response.json();
      return data[0];
    } else {
      throw new Error(response.statusText)
    }
  }
}

module.exports = CrossrefMatcher;
