const scholarly = require('scholarly');
const inquirer = require("inquirer");
const crossrefMatcher = require('./crossref');
const process = require("process");
const debug = require('debug')('scholar')

/**
 * This matcher is almost useless if you are using it as-is, since Google
 * Scholar will block you after a few requests. It will only work efficiently if used
 * in combination with a proxy service that distributes the requests over a variety of IP
 * addresses.
 */
class GoogleScholarMatcher {

  /**
   * Matches by (segmented) reference string and returns *possibly incomplete* Zotero-formatted data or null if no
   * match could be found
   * @param {string} refstring
   * @param {{lastNames:[], year:number, titleWords:[]}} refseg
   * @returns {Promise<Object|0>}
   */
  static async match(refstring, refseg){
    let {lastNames, year, titleWords} = refseg;
    let result = await scholarly.search(refstring);
    let choices = [];
    for (let data of result) {
      let match = data.url.match(/\/(10\.[^/]+\/[^/]+)/);
      if (match) {
        data.DOI = match[1];
      }
      let foundMatch =
        year === Number(data.year) &&
        titleWords.some(word => data.title.toLocaleLowerCase().includes(word)) &&
        data.authors.some(author => author.toLocaleLowerCase().includes(lastNames[0].toLocaleLowerCase()));
      if (foundMatch) {
        let name = `${data.authors.join("; ")} (${data.year}), ${data.title}`;
        choices.push({
          name,
          value: data
        });
      }
    }
    debug(`Using ${choices.length} of ${result.length} results.`);
    let data;
    switch (choices.length) {
      case 0:
        return null;
      case 1:
        data = choices[0].value;
        break;
      default:
        if (process.env.MATCH_NON_INTERACTIVE) {
          return null;
        }
        choices.unshift({
          name: "--- no match ---",
          value: null
        });
        ({data} = await inquirer.prompt([{
          type: "list",
          name: "data",
          message: "Select the matching Google Scholar reference:",
          choices
        }]));
    }
    if (data.DOI) {
      return await crossrefMatcher.lookup({DOI:data.DOI});
    }
    return {
      creators: data.authors.map(author => ({
        creatorType: "author",
        name: author.replace(/[^\p{L}\p{P}\p{Whitespace}]/ug,"")
      })),
      date: String(data.year),
      title: data.title.replace(/[^\p{L}\p{P}\p{Whitespace}]/ug,""),
      url: data.url || "",
      abstractNote: data.description || "",
      extra: refstring
    };
  }
}

module.exports = GoogleScholarMatcher;
