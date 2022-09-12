const fetch = require("node-fetch");

/**
 * VIAF (Virtual International Authority File)
 * @see https://www.oclc.org/developer/develop/web-services/viaf.en.html
 * @see https://www.oclc.org/developer/develop/web-services/viaf/authority-cluster.en.html
 */

module.exports = {
    name: "VIAF (Virtual International Authority File)",
    async find(match_name) {
        match_name = match_name.replace(".","");
        let query = encodeURIComponent(`local.personalNames all ${match_name}`);
        let queryUrl = `https://viaf.org/viaf/search?query=${query}&httpAccept=application/json`
        let response = await fetch(queryUrl);
        let result = await response.json();
        if (!result || typeof result.searchRetrieveResponse != "object") {
            throw new Error("Invalid response from server");
        }
        if (Number(result.searchRetrieveResponse.numberOfRecords) === 0) {
            return [];
        }
        let records = result.searchRetrieveResponse.records;
        return records
            .map(item => {
                let recordData = item.record.recordData;
                let nameData = recordData.mainHeadings.data;
                if (!Array.isArray(nameData)) {
                    nameData = [nameData];
                }
                let variants = nameData
                    .map(entry => entry.text)
                    .filter(entry => entry === match_name || entry.slice(0, match_name.length+1).match(new RegExp(match_name + "[. ]")))
                if (variants.length === 0) {
                    return null;
                }
                let id = recordData.viafID;
                let data = {
                    variants,
                    links: [{
                        id: "viaf:" + id,
                        schema: "http://viaf.org/VIAFCluster",
                        url: `https://viaf.org/viaf/${id}/?httpAccept=application/json`
                    }]
                };
                if (recordData.titles && recordData.titles.work) {
                    let works = recordData.titles.work;
                    if (!Array.isArray(works)){
                        works = [works];
                    }
                    data.info = [{
                        works: works.map(work => work.title)
                    }];
                }
                if (recordData.birthDate && recordData.birthDate !== "0") {
                    data.birthDate = recordData.birthDate;
                }
                return data;
            }).filter(entry => Boolean(entry));
    }
};