const fetch = require("node-fetch");

/**
 * DNB Normdaten, via dariah.eu
 * @see https://wiki.de.dariah.eu/display/publicde/DARIAH-DE+Normdatendienste#DARIAHDENormdatendienste-GemeinsameNormdatei(GND)
 */
module.exports = {
    name: "Gemeinsame Normdatei (GND) via dariah.eu",
    async find(match_name) {
        let lastname = match_name.match(/^([^,]+)/);
        let query = encodeURIComponent(`${lastname ? lastname[1] : match_name}`);
        let queryUrl = `https://ref.de.dariah.eu//pndsearch/json?ln=${query}`;
        let response = await fetch(queryUrl);
        let result = await response.json();
        if (!result || !result.person) {
            return [];
        }
        let records = result.person;
        if (typeof records == "object") {
            records = [records];
        }
        return records
            .map(record => {
                let variants = [record.match_name];
                if (record.variant) {
                    if (Array.isArray(record.variant)) {
                        variants = variants.concat(record.variant);
                    } else {
                        variants.push(record.variant);
                    }
                }
                variants = variants
                    .filter(entry => Boolean(entry))
                    .filter(entry => entry === match_name || entry.slice(0, match_name.length+1).match(new RegExp(match_name + "[. ]")))
                    .filter((value, index, self) => self.indexOf(value) === index);
                if (variants.length === 0) {
                    return null;
                }
                let id = record.id.slice(4);
                let data = {
                    variants,
                    links: [{
                        id: "pnd:" + id,
                        schema: "https://ref.de.dariah.eu/pndsearch/json",
                        url: `https://ref.de.dariah.eu/pndsearch/json?id=${id}`
                    }, {
                        id: "pnd:" + id,
                        schema: "http://www.w3.org/TR/turtle",
                        url: record.link.target + "/about/Ids"
                    }]
                };
                if (record.info) {
                    data.info = [{ bio: record.info.trim() }];
                    let birthDateMatch = record.info.match(/\*([0-9]{4})/);
                    if (birthDateMatch) {
                        data.birthDate = birthDateMatch[1];
                    }
                }
                return data;
            }).filter(item => Boolean(item));
    }
};