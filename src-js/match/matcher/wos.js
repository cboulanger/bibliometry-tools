const {promises: fsp} = require("fs");

class WosMatcher {

  /**
   * Creates a file with a WOS/ISI header
   * @param outfile
   * @param encoding
   * @returns {Promise<void>}
   */
  static async createFile(outfile, encoding) {
    const header = [
      "FN Thomson Reuters Web of Science™",
      "VR 1.0",
      ""
    ];
    await fsp.writeFile(outfile, header.join("\n"), {encoding});
  }

  /**
   * Appends a WOS/ISI record to the given file.
   * @param outfile
   * @param item
   * @param {String[]} CR
   * @param encoding
   * @returns {Promise<void>}
   */
  static async appendRecord(outfile, item, CR, encoding) {
    const fields = this.createRecord(item, CR);
    let record = Object.entries(fields)
      .filter(([, value]) => value && Boolean(String(value).trim()))
      .map(([key, value]) => `${key} ${value}`)
      .join("\n") + "\nER\n\n";
    await fsp.appendFile(outfile, record, {encoding});
  }

  /**
   * Given Zotero item data, return a map which represents a record in a ISI-formatted WOS export file
   * todo only works for journal articles so far
   * @param {Object} data
   * @param {Array} CR The CR field contents as an array
   */
  static createRecord(data, CR=[]) {
    let authors = data.creators;
    let AU = authors.map(a => a.lastName? (a.lastName + (a.firstName ? (", " + a.firstName[0]):"")) : a.name);
    let AF = authors.map(a => a.lastName? (a.lastName + (a.firstName ? (", " + a.firstName):"")) : a.name);
    let PY = Number((new Date(data.date)).getFullYear());
    if (isNaN(PY)) {
      try {
        PY = Number(data.date.match(/[12][9012][0-9]{2}/)[0]);
      } catch (e) {
        PY = 2999
      }
    }
    let fields = {
      "PT": "J",
      "AU": AU.join("\n   "),
      "AF": AF.join("\n   "),
      "TI": data.title || "--NO TITLE--",
      "SO": data.publicationTitle,
      "LA": data.language,
      "DT": "Article",
      "DE": data.tags.map(tag => tag.tag).join("; "),
      "AB": (data.abstractNote || "").replace(/\n|<\/?[^>]+>/g,""),
      // todo C1 affiliations
      // Milojevic, Stasa; Sugimoto, Cassidy R.; Dinga, Ying] Indiana Univ, Sch Informat & Comp, Bloomington, IN 47405 USA.
      "J9": data.publicationTitle,
      "JI": data.publicationTitle,
      "PD": data.date,
      PY,
      "VL": data.volume,
      "IS": data.issue,
      "BP": (data.pages || "").split("-")[0],
      "DI": data.DOI
    };
    if (Array.isArray(CR) && CR.length) {
      fields.CR = CR.join("\n   ");
      fields.NR = String(CR.length);
    }
    return fields;
  }

  /**
   * Given Zotero item data, returns string content for the CR field as part of a record in the Web of Science ISI export file format
   * @param {Object} item
   * @returns {string}
   */
  static createCrFieldEntry(item) {
    try {
      let cit = [];
      let creator = item.creators[0];
      if (creator.lastName) {
        cit.push(creator.lastName + (creator.firstName ? " " + creator.firstName[0] : ""));
      } else {
        cit.push(creator.name);
      }
      let m = String(item.date).match(/[12][9012][0-9]{2}/);
      cit.push(m ? m[0] : item.date);
      if (item.publicationTitle) {
        // journal article
        cit.push(item.publicationTitle);
        if(item.volume) cit.push("V" + item.volume);
        if(item.pages) cit.push("P" + item.pages.split("-")[0]);
        if(item.DOI) cit.push("DOI " + item.DOI);
      } else {
        // everything else
        cit.push(item.title)
        if (item.ISBN) {
          cit.push("ISBN " + item.ISBN);
        }
      }
      return cit.join(", ");
    } catch (e) {
      console.debug(item);
      throw e;
    }
  }
}

module.exports = WosMatcher;
