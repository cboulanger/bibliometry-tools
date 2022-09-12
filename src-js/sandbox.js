const couchbase = require("couchbase");
const { createClient } = require('webdav');
const Gauge = require("gauge");
const dotenv = require("dotenv");
const process = require('process');
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const {AbbyyOcr} = require('@cboulanger/abbyy-cloud-ocr');

const {Server, Library} = require("./zotero/index");


// load environment variables
const ENV = {
  COUCH_USER:"",
  COUCH_PASSWD:"",
  COUCH_REMOTE_URL:"",
  COUCH_REMOTE_USER:"",
  COUCH_REMOTE_PASSWD:"",
  ZOTERO_API_KEY:"",
  ZOTERO_GROUP:"",
  EZPROXY_USER:"",
  EZPROXY_PASS:"",
  WEBDAV_URL:"",
  WEBDAV_USER:"",
  WEBDAV_PASSWD:"",
  ABBYY_SERVICE_URL:"",
  ABBYY_APP_ID:"",
  ABBYY_APP_PASSWD:""
}
const envvars = dotenv.config();
if (envvars.error) {
  throw envvars.error
}
for (let key of Object.keys(ENV)) {
  if (!envvars.parsed[key]) {
    throw new Error(`You must set the environment variable '${key}' in .env`);
  }
  ENV[key] = envvars.parsed[key];
}

let localCluster;
let remoteCluster;
let gauge;

const logger = console;

const webDavClient = createClient(process.env.WEBDAV_URL, {
  username: process.env.WEBDAV_USER,
  password: process.env.WEBDAV_PASSWD
});

const zoteroServer = new Server(process.env.ZOTERO_API_KEY);


const sandbox = {

  ENV,

  get gauge() {
    return gauge ? gauge : gauge = new Gauge();
  },

  /**
   * Returns the names of the buckets as constants
   * @returns {{AUTHORS: string, ARTICLES: string, DE_GRUYTER_JATS: string, DIGIZEIT_DC: string}}
   */
  get buckets() {
    return {
      DE_GRUYTER_JATS: "de-gruyter-jats",
      DIGIZEIT_DC: "digizeit-dc",
      AUTHORS: "authors",
      ARTICLES: "articles",
      ZOTERO: "zotero"
    };
  },

  /**
   * Returns the local cluster instance
   * @returns {Promise<Cluster>}
   */
  async getLocalCluster() {
    if (!localCluster) {
      localCluster = await couchbase.connect("couchbase://127.0.0.1:8091", {
        username: process.env.COUCH_USER,
        password: process.env.COUCH_PASSWD,
      });
    }
    return localCluster;
  },

  /**
   * Returns the remore cluster instance
   * @returns {Promise<Cluster>}
   */
  async getRemoteCluster() {
    return await couchbase.connect(`couchbase://${process.env.COUCH_REMOTE_URL}`, {
        username: process.env.COUCH_REMOTE_USER,
        password: process.env.COUCH_REMOTE_PASSWD,
      });
  },

  /**
   *
   * @param {Cluster} cluster
   * @param {string} bucketName
   * @param {Boolean} dropIfExists
   * @returns {Promise<void>}
   */
  async createBucket(cluster, bucketName, dropIfExists=false) {
    let gauge = this.gauge;
    try {
      await cluster.buckets().getBucket(bucketName);
      if (dropIfExists) {
        // exists and should be deleted
        try {
          gauge.show(`Dropping ${bucketName}...`);
          await cluster.buckets().dropBucket(bucketName);
        } catch (e) {
          console.error(e);
        }
      } else {
        // exists and should not be deleted
        return;
      }
    } catch (e) {
      // bucket does not exist, create
    }

    gauge.show(`Creating ${bucketName}...`);
    let options = {
      name: bucketName,
      flushEnabled: false,
      replicaIndexes: false,
      ramQuotaMB: 200,
      numReplicas: 0,
      bucketType: couchbase.BucketType.Couchbase,
    };
    await cluster.buckets().createBucket(options);
    await cluster.queryIndexes().createPrimaryIndex(bucketName);
  },

  /**
   * Return the buckets default collection
   * @param buckeName
   * @returns {Promise<Collection>}
   */
  async getBucketDefaultCollection(buckeName) {
    return (await sandbox.getLocalCluster()).bucket(buckeName).defaultCollection();
  },

  /**
   * Downloads a file from the given url to the local path
   * @param {String} sourceUrl
   * @param {String} targetPath
   * @returns {Promise<void>}
   */
  async download(sourceUrl, targetPath) {
    const {createWriteStream} = require('fs');
    const {pipeline} = require('stream');
    const {promisify} = require('util');
    const fetch = require('node-fetch');
    const streamPipeline = promisify(pipeline);
    const response = await fetch(sourceUrl);
    if (!response.ok) throw new Error(`Unexpected response ${response.statusText}`);
    await streamPipeline(response.body, createWriteStream(targetPath));
  },

  /**
   * Returns the client for the full text repository
   * @returns {WebDAVClient}
   */
  getWebDavClient() {
    return webDavClient;
  },

  /**
   * Downloads a file from the webdav file repository
   * @param {String} fileName
   * @param {String} targetPath
   * @returns {Promise<void>}
   */
  async downloadFromWebDav(fileName, targetPath) {
    await new Promise( (resolve, reject) => {
      sandbox.getWebDavClient()
        .createReadStream(fileName)
        .on('error', reject)
        .pipe(fs.createWriteStream(targetPath))
        .on('finish', resolve)
        .on('error', reject);
    });
  },

  async uploadToWebDav(sourcePath, fileName= null) {
    if (!(await fsp.stat(sourcePath)).isFile()) {
      throw new Error(`${sourcePath} is not a file.`);
    }
    if (!fileName) {
      fileName = path.basename(sourcePath);
    }
    await new Promise( (resolve, reject) => {
      fs.createReadStream(sourcePath)
        .on('error', reject)
        .pipe(sandbox.getWebDavClient().createWriteStream(fileName))
        .on('finish', resolve)
        .on('error', reject);
    });
  },

  /**
   * Checks if two values are the same. If the values are objects or array, their items/properties are compared.
   * @param a
   * @param b
   * @returns {boolean}
   */
  isDeepEqual(a,b) {
    if (typeof a == "array") {
      if (a.length === 0 && b.length === 0) return true;
      return a.length === b.length && a.every((value, index) => this.isDeepEqual(value, b[index]));
    } else if (typeof a == "object") {
      if (Object.keys(a).length !== Object.keys(b).length) return false;
      return Object.keys(a).every(key => this.isDeepEqual(a[key], b[key]));
    }
    return a === b;
  },

  /**
   * Runs the given command and returns an object containing information on the
   * `exitCode`, the `output`, potential `error`s, and additional `messages`.
   *
   * @param {String|Object} opt
   *    If string, the current working directory. If object, a map of options:
   *   `{String} cwd` - The working dir
   *   `{Function} log` / `{Function}  error` Functions that will receive stdout and stderr;
   *   `{String} cmd` The CLI command
   *   `{String[]} args` The CLI arguments
   *   `{Object} env` Map of environment variables
   *
   * @param {String?} args One or more command line arguments, including the
   * command itself, omitted if first argument is a map
   *
   * @return {{exitCode: Number, output: String, error: *, messages: *}}
   */
  async runCommand(opt, ...args) {
    let options = {};
    if (typeof opt == "object") {
      options = opt;
    } else {
      args = args.filter(value => {
        if (typeof value == "string") {
          return true;
        }
        if (!options) {
          options = value;
        }
        return false;
      });
      if (!options.cwd) {
        options.cwd = opt;
      }
      if (!options.cmd) {
        options.cmd = args.shift();
      }
      if (!options.args) {
        options.args = args;
      }
    }
    if (!options.error) {
      options.error = console.error;
    }
    if (!options.log) {
      options.log = console.log;
    }
    return new Promise((resolve, reject) => {
      let env = process.env;
      if (options.env) {
        env = Object.assign({}, env);
        Object.assign(env, options.env);
      }
      let proc = require('child_process')
        .spawn(options.cmd, options.args, {
          cwd: options.cwd,
          shell: true,
          env: env
        });
      let result = {
        exitCode: null,
        output: "",
        error: "",
        messages: null
      };
      proc.stdout.on("data", data => {
        data = data.toString().trim();
        options.log(data);
        result.output += data;
      });
      proc.stderr.on("data", data => {
        data = data.toString().trim();
        options.error(data);
        result.error += data;
      });
      proc.on("close", code => {
        result.exitCode = code;
        resolve(result);
      });
      proc.on("error", err => {
        reject(err);
      });
    });
  },

  /**
   * logger
   */
  logger,

  /**
   * returns the zoteroApi object configured with the user's API key.
   * @returns {Server}
   */
  getZoteroServer() {
    return zoteroServer;
  },

  /**
   * Returns a preconfigured Library instance
   * @param {String} libraryId
   * @returns {Library}
   */
  getZoteroLibrary(libraryId) {
    return new Library(zoteroServer,libraryId);
  },

  /**
   * Sets up and returns the OCR client
   * @return {AbbyyOcr}
   */
  createAbbyyClient(options={}){
    options.serviceUrl = options.serviceUrl || ENV.ABBYY_SERVICE_URL;
    options.appId = options.appId || ENV.ABBYY_APP_ID;
    options.password = options.password || ENV.ABBYY_APP_PASSWD;
    return new AbbyyOcr(options.appId, options.password, options.serviceUrl);
  }
}

module.exports = sandbox;
