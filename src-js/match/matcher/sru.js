const {default: createClient} = require("@natlibfi/sru-client");
const {Parser: XMLParser} = require("xml2js");
const inquirer = require("inquirer");
const fetch = require("node-fetch");
const levenshtein = require('fast-levenshtein');
const process = require("process");
const debug = require('debug')('sru');
const debugVerbose = require('debug')('sru-verbose');

const levDistMax = 10; // todo generalize this

const sruServers = [
  {
    name: "Deutsche Nationalbibliothek",
    url: "https://services.dnb.de/sru/dnb",
    version: "1.1",
    recordschema: "oai_dc",
    formatQuery: (author, year, titleWords) => `atr=${author} and jhr=${year} and (title=${titleWords[0]} or title=${titleWords[1]})`
  }, {
    name: "Library of Congress",
    url: "http://z3950.loc.gov:7090/voyager",
    version: "1.1",
    recordschema: "dc",
    formatQuery: (author, year, titleWords) => `cql.anywhere=${author} ${titleWords.join(' ')} and (dc.date=c${year} or dc.date=${year})`
  }, {
    name: "Bibliothèque nationale de France",
    url: "http://catalogue.bnf.fr/api/SRU",
    version: "1.2",
    recordschema: "dublincore",
    formatQuery: (author, year, titleWords) => `bib.anywhere all "${author} ${year} ${titleWords.join(' ')}"`
  }
];

class SruMatcher {

  /**
   * Runs a query at the server with the given details. Returns a JSON serialization of the XML data, wrapped in
   * a `root.dc` object.
   * @param {Object} server
   * @param {string} query
   * @returns {Promise<Object>}
   */
  static sruQuery(server, query) {
    return new Promise((resolve, reject) => {
      const sruClient = createClient({
        url: server.url,
        recordSchema: server.recordschema,
        version: server.version,
        retrieveAll: true
      });
      let records = [];
      sruClient.searchRetrieve(query)
        .on('record', record => records.push(record))
        .on('end', () => new XMLParser().parseString(
          '<root>' + records.join("\n").replace(/^<\?[^>]+>$/g,"") + '</root>',
          (err, obj) => err ? reject(err) : resolve(obj))
        )
        .on('error', reject);
    });
  }

  /**
   * Matches by (segmented) reference string and returns complete Zotero-formatted data or null if no
   * match could be found
   * @param {string} refstring
   * @param {{lastNames:[], year:number, titleWords:[]}} refseg
   * @returns {Promise<Object|0>}
   */
  static async match(refstring, refseg ){
    let {lastNames, year, titleWords, title} = refseg;
    let choices = [];
    let allItems =[];
    // todo: check for language of title and use only matching serves
    for (let sruServer of sruServers) {
      let query = sruServer.formatQuery(lastNames[0], year, titleWords.slice(0, 3));
      debug(`${sruServer.url}: ${query}`);
      let sruResult;
      try {
        sruResult = await this.sruQuery(sruServer, query);
      } catch (e) {
        console.error(`Error contacting ${sruServer.name}: ${e.message}`);
        continue;
      }
     //console.dir(sruResult, {depth:10})
      if (!sruResult.root) {
        debug('No results');
        continue;
      }
      let items = Object.values(sruResult.root)[0];
      allItems = allItems.concat(items);
      // namespace-agnostic retrieval of value in JSON-serialized XML
      function xmlVal(node, prop) {
        for (let [p,v] of Object.entries(node)) {
          let [ns, tag] = p.split(":");
          if (!tag) tag = ns;
          if (tag === prop) {
            if (Array.isArray(v)) {
              return v.map(i => (i && typeof i == "object" && "_" in i) ? i._ : i);
            }
            return (v && typeof v == "object" && "_" in v) ? v._ : v;
          }
        }
      }
      for (let item of items) {
        try {
          let yp;
          try {
            yp = Number(xmlVal(item,'date')[0].match(/[12][9012][0-9]{2}/)[0]);
          } catch (e) {
            debugVerbose({reason: "no valid year information", error: e.message, item});
            continue;
          }
          // year must match
          if (year !== yp) {
            debugVerbose({reason: "year does not match", year, yp});
            continue;
          }
          let au = xmlVal(item, 'creator');
          let auJson = JSON.stringify(au).toLocaleLowerCase();
          // author info must at least contain one last name
          if (!lastNames.some(name => auJson.includes(name))) {
            debugVerbose({reason: "last names does not match", auJson, lastNames});
            continue;
          }
          let ti = xmlVal(item, 'title')[0];

          // at least contain one title word
          // if (!titleWords.some(word => ti.toLocaleLowerCase().includes(word))) {
          //   debugVerbose({reason: "title words do not match", ti, titleWords});
          //   continue;
          // }

          // use the shorter part for Levenshtein distance analysis
          let l = Math.min(title.length, ti.length);
          if (levenshtein.get(title.substr(0,l), ti.substr(0,l)) > levDistMax) {
            debugVerbose({reason: "levenshtein distance in titles to large", ti, title});
            continue;
          }
          let identifier = xmlVal(item, 'identifier');
          let isbn;
          if (Array.isArray(identifier)) {
            isbn = identifier
              .map(i => typeof i == "string" && i.match(/([0-9-X]{10,})/))
              .filter(i => Boolean(i))
              .map(i => i[1])
              .shift();
          }
          // add if not a duplicate
          if (isbn && !choices.some(c => c.value.replace("-","") === isbn.replace("-",""))) {
            choices.push({
              name: `${au} (${yp}), ${ti}`,
              value: isbn
            });
          } else {
            debugVerbose({reason: "no isbn information found", identifier, item});
          }
        } catch (e) {
          console.error(e.message);
          debug({reason: "item crashes checks", error:e, item});
        }
      } // end for items
      if (choices.length) {
        // if one server has produced a result, no need to query the others.
        break;
      }
    } // end for server
    debug(`Using ${choices.length} of ${allItems.length} results`);
    let ISBN;
    switch (choices.length) {
      case 0:
        return null;
      case 1:
        ISBN = choices[0].value;
        break;
      default:
        if (Number(process.env.MATCH_NON_INTERACTIVE)) {
          debug({reason: "multiple choices in non-interactive mode, no match", choices});
          return null;
        }
        choices.unshift({
          name: "--- no match ---",
          value: null
        });
        ({ISBN} = await inquirer.prompt([{
          type: "list",
          name: "ISBN",
          message: "Select the matching library reference:",
          choices
        }]));
    }
    return this.lookup({ISBN});
  }

  /**
   * Looks up by specific Zotero item fields and returns completed Zotero-formatted item data
   * @param {Object} item
   * @returns {Promise<Object>}
   */
  static async lookup(item){
    if (!item.ISBN) {
      throw new Error("Item data does not contain ISBN field");
    }
    // use translation server to query data from crossref
    let response = await fetch(`${process.env.ZOTERO_TRANSLATION_SERVER_URL}/search`, {
      method:"POST",
      headers: {"Content-Type":"text/plain"},
      body: item.ISBN
    });
    if (response.ok) {
      let data = await response.json();
      return data[0];
    } else {
      throw new Error(response.statusText)
    }
  }
}

module.exports = SruMatcher;
