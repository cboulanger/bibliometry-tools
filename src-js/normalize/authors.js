// authority datasources
const viaf = require("./normalize/authority/viaf");
const dnb = require("./normalize/authority/dnb");

/**
 * @param {Cluster} cluster
 * @param {Gauge} gauge
 */
async function normalize(cluster, gauge) {
  gauge.show("Extracting authors from JATS metadata...");
  let {rows:authors} = await cluster.query(
    'SELECT DISTINCT contrib.name.surname AS lastname, contrib.name.`given-names` as firstnames, ' +
    '`' +  buckets.DE_GRUYTER_JATS + '`.article.front.`article-meta`.`pub-date`[0].year as year ' +
    'FROM `' + buckets.DE_GRUYTER_JATS + '` ' +
    'UNNEST article.front.`article-meta`.`contrib-group`.contrib ' +
    'ORDER BY LOWER(contrib.name.surname) ASC');
  const collection = cluster.bucket(buckets.AUTHORS).defaultCollection();
  await cluster.queryIndexes().createPrimaryIndex(buckets.AUTHORS, {ignoreIfExists:true});

  gauge.show("Extracting authors from DigiZeitschriften metadata...");
  let {rows:authors2} = await cluster.query(
    'select distinct raw `dc:creator` ' +
    'from `' + buckets.DIGIZEIT_DC + '` ' +
    'unnest `dc:creator` ' +
    'where `dc:creator`');
  for (let author of authors2) {
    if (!author) continue;
    let m = author.match(/([^ ]+)[ ]*,[ ]*([^ ]+)/);
    if (m) {
      authors.push({
        lastname: m[1],
        firstnames: m[2],
        year: null
      });
    }
  }
  let counter = 0;
  let total = authors.length;
  for (let {lastname, firstnames, year} of authors) {
    let match_name = `${lastname}, ${firstnames}`;
    let {rows} = await cluster.query(
      "SELECT RAW COUNT(variants) FROM `" + buckets.AUTHORS + "` " +
      "WHERE ANY variant in variants " +
      "SATISFIES variant like '" + match_name + "' OR variant like '" + match_name + ".' END;");
    console.log(match_name, rows);
    if (rows[0] > 0) {
      gauge.show(`${match_name} exists.`, counter/total);
    } else {
      let id;
      let record = null;
      for (let datasource of [dnb, viaf]) {
        gauge.show(`Searching for ${match_name} in ${datasource.name}...`, counter/total);
        id = match_name;
        let records = await datasource.find(match_name);
        for (let r of records) {
          if (r.birthDate) {
            birthDate = parseInt(r.birthDate.match(/^([0-9]{4})/));
            if (year && birthDate && !isNaN(birthDate) && birthDate - year < -80) {
              //console.log(`Too old? Born ${birthDate}, written ${year}: ${birthDate-year}`);
              continue;
            }
            id += "_" + birthDate;
            try {
              let {content} = await collection.get(id);
              for (let [key, value] of Object.entries(r)) {
                if (content[key] === undefined) {
                  content[key] = value;
                } else if (Array.isArray(content[key])) {
                  content[key] = content[key]
                    .concat(r[key])
                    .filter((value, index, self) => self.indexOf(value) === index);
                }
              }
              r = content;
            } catch (e) {}
          } else {
            id += "_" + Math.random().toString().slice(-5);
          }
          record = r;
          if (!record.variants.includes(match_name)) {
            record.variants.push(match_name);
          }
        }
      }
      if (!record) {
        record = {
          "variants": [match_name]
        };
      }
      if (!id) {
        id = match_name + "_" + Math.random().toString().slice(-5);
      }
      id = id.replace(/ /g, "_").replace(/[,.]/g,"");
      gauge.show(`Saving ${match_name} with id ${id} ...`, counter/total);
      console.log({id,record});
      await collection.upsert(id, record);
    }
  }
}

module.exports = {
  normalize
}