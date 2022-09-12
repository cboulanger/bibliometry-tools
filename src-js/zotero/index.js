/**
 * High-Level Zotero API, based on https://github.com/tnajdek/zotero-api-client
 * Provides classes that model the Zotero entities (libraries, groups,
 * collections, items, attachments, etc.). Note that most instance methods are
 * asynchronous and rely on lazy loading of the entity data, i.e. data is
 * fetched from the server only when necessary.
 */

const zotero = require('zotero-api-client');
const fetch = require('node-fetch');
const path = require('path');
const {default:ow} = require('ow');
const mime = require('mime-types');
const fs = require("fs");
const debug = require('debug')('zotero');

/**
 * The Server class contains utility methods to deal with the Zotero Server API
 * You do not usually need to interact with it, except for instantiating it
 * with the Zotero API key.
 */
class Server {

  /**
   * Configures the instance with the API key
   * @param {String} key
   */
  constructor(key) {
    this.__api_key = key;
    this.retryAttempts = 5;
    this.resetState();
  }

  /**
   * Reset cached library state so that it will be re-fetched from zotero.org
   */
  resetState() {
    this.__state = {
      keyAccess: null,
      groupVersions: null,
      groupData: null
    };
  }

  /**
   * Returns a (low-level) zotero-api-client api instance. Should not be used directly.
   * @returns {Object}
   */
  _api() {
    return zotero(this.__api_key);
  }

  /**
   * Returns the response to /keys/current request
   * @returns {Promise<{userID}>}
   */
  async getKeyAccess() {
    if (!this.__state.keyAccess) {
      this.__state.keyAccess = await (await fetch(`https://api.zotero.org/keys/current`, {
        headers: {"Zotero-API-Key": this.__api_key}
      })).json();
    }
    return this.__state.keyAccess;
  }

  /**
   * Returns the user id of the owner of the API key
   * @returns {Promise<*>}
   */
  async getUserId() {
    return (await this.getKeyAccess()).userID;
  }

  /**
   * Returns a map of user-accessible groups (keys) and their latest-modified-version (values)
   * @returns {Promise<Object>}
   */
  async getGroupVersions() {
    if (!this.__state.groupVersions) {
      let userId = await this.getUserId();
      this.__state.groupVersions = await (await fetch(`https://api.zotero.org/users/${userId}/groups?format=versions`, {
        headers: {"Zotero-API-Key": this.__api_key}
      })).json();
    }
    return this.__state.groupVersions;
  }

  /**
   * Returns a list of the ids of the groups to which the owner of the current API key has access.
   * @returns {Promise<String[]>}
   */
  async getGroupIds() {
    return Object.keys(await this.getGroupVersions());
  }

  /**
   * Returns data on the given group
   * FIXME: Check access!!
   * @param {Number} groupId
   * @returns {Promise<{}>}
   */
  async getGroupData(groupId) {
    if (!this.__state.groupData) {
      this.__state.groupData = {};
    }
    if (!this.__state.groupData[groupId]) {
      this.__state.groupData[groupId] = await (await fetch(`https://api.zotero.org/groups/${groupId}`, {
        headers: {"Zotero-API-Key": this.__api_key}
      })).json();
    }
    return this.__state.groupData[groupId];
  }

  /**
   * Returns the data template for the given item type
   * @param {String} itemType
   * @return {Promise<Object>}
   */
  async getTemplate(itemType) {

    if (!this.__templates){
      this.__templates = {};
    }
    let opts = {
      retry: this.retryAttempts
    };
    if (!this.__templates[itemType]) {
      let response = await this._api().template(itemType).get(opts);
      this.__templates[itemType] = response.getData();
    }
    // clone template
    return JSON.parse(JSON.stringify(this.__templates[itemType]));
  }

  /**
   * Returns a random 32-character hash required for write operations
   * @returns {string}
   */
  createWriteToken() {
    let writeToken="";
    do {
      writeToken += Math.random().toString(36).substr(2)
    } while (writeToken.length < 32);
    return writeToken.substr(0, 32);
  }

  /**
   * Returns a new Library instance for the given id.
   * @param {string} libraryId
   * @returns {Library}
   */
  library(libraryId) {
    return new Library(this, libraryId);
  }
}

class Library {
  /**
   * The library instance constructor
   * @param {Server} server
   * @param {String} libraryId
   */
  constructor(server, libraryId) {
    this.id = libraryId;
    this.server = server;
  }

  /**
   * Returns the low-level api object preconfigured for this library. Should not
   * be used directly.
   * @returns {*}
   */
  _api() {
    return this.server._api().library(this.id);
  }

  /**
   * Returns information on the library
   * @returns {Promise<{name: string, id: string, type: string}>}
   */
  async getInfo() {
    let name, type, id = this.id;
    if (this.id[0] === "g") {
      let data  = (await this.server.getGroupData(Number(id.slice(1)))).data;
      name = data.name;
      type = "group"
    } else {
      name = "User Library"
      type = "user"
    }
    return {
      name,
      type,
      id
    };
  }

  /**
   * Returns an array of keys of records of the given type (items, collections, ...)
   * which have changed for since the given version number
   * @see https://www.zotero.org/support/dev/web_api/v3/syncing#i_get_updated_data
   * @param {String} type
   * @param {Number} version
   * @returns {Promise<{version:Number, keys:string[]}>}
   */
  async getModifiedSince(type, version) {
    this.assertValidType(type);
    let opts = {
      since: version,
      format: "keys",
      retry: this.server.retryAttempts
    };
    let response = await this._api()[type]().get(opts);
    if (version === response.getVersion()) {
      return {
        version,
        keys: []
      };
    }
    version = response.getVersion();
    let keys = (await response.getData().text()).trim().split("\n").filter(key => Boolean(key));
    return { version, keys };
  }

  /**
   * Returns a map containing as keys the type names (items, collections, ...) and
   * as values an array of the keys of type records which have been deleted since the
   * given version. The map also contains the key `version` with the current version
   * of the library.
   * which have been deleted since the given version number
   * @see https://www.zotero.org/support/dev/web_api/v3/syncing#ii_get_deleted_data
   * @returns {Promise<{ version: Number, collections: String[], items: String[], searches: String[], tags: String[], settings: String[] }>}
   */
  async getDeletedSince(version) {
    let opts = {
      retry: this.server.retryAttempts
    };
    let response = await this._api().deleted(String(version)).get(opts);
    let data = response.getData();
    data.version = response.getVersion();
    return data;
  }


  /**
   * Retrieves the json data of the records with the given type (collection, items, ...), having the given
   * key.
   * @param {String} type
   * @param {String[]} keys
   * @returns {Promise<Object[]>}
   */
  async getRecordData(type, keys) {
    this.assertValidType(type);
    let opts = {
      itemKey:keys.join(","),
      retry: this.server.retryAttempts
    };
    return (await this._api()[type]().get(opts)).getData();
  }

  /**
   * Checks that the type is supported by the library
   * @param type
   * @throws Error if type is not supported
   */
  assertValidType(type) {
    if (!["items","collections"].includes(type)) {
      throw new Error(`Type '${type}' is not supported.`);
    }
  }

  /**
   * Returns a new instance representing an existing item. To load its data, you need to use {@link Item#fetch()}.
   * @param {String} key
   * @returns {Item}
   */
  item(key) {
    return new Item(this, key);
  }

  /**
   * Returns a new instance representing an existing attachment item.
   * @param {String} key
   * @returns {Attachment}
   */
  attachment(key) {
    return new Attachment(this, key);
  }

  /**
   * Returns a new instance representing an existing collection item.
   * @param {String} key
   * @returns {Collection}
   */
  collection(key) {
    return new Collection(this, key);
  }

  /**
   * Similar to `mkdir -p`, makes sure that the given "folder path" in the collections exists.
   * Returns the collection at the end of the path.
   * @param {String} collPath The path, separated by "/"
   * @return {Promise<Collection>}
   */
  async createCollectionPath(collPath) {
    let collection;
    for (let segment of collPath.split("/")) {
      collection = await Collection.create(this, segment, collection ? collection.key : false);
    }
    return collection;
  }

  /**
   * Creates the item with the given data in the given collection path. The data
   * @param {String} collectionPath The path, separated by "/"
   * @param {Object} data Usually result of a coll to  {@link Server#getTemplate()} which has been
   * populated with the item data
   * @return {Promise<Item>}
   */
  async createItemInCollection(collectionPath, data) {
    let collection = await this.createCollectionPath(collectionPath);
    data.collections = [collection.key];
    return Item.create(this, data);
  }

  /**
   * Searches the library for items that match the given query. The query is passed as the `q` parameter
   * to the server (see https://www.zotero.org/support/dev/web_api/v3/basics#search_parameters), so all
   * limitations of this query method apply.
   * Returns an array of item including their data.  The maximal number of search results are 100.
   * TODO rewrite as async generator!
   * @param query
   * @returns {Promise<Item[]>}
   */
  async search(query) {
    let opts = {
      q: query,
      retry: this.server.retryAttempts
    };
    let result = await this._api().items().get(opts);
    return result.getData().map(data => new Item(this, data.key, data));
  }
}

/**
 * The parent class of all Zotero objects
 */
class Entity {

  /**
   * Abstract method to be overridden by subclasses
   * @abstract
   */
  static async create() {
    throw new Error("Not implemented for Entity, use a specialized subclass");
  }

  /**
   * @param {Library} library
   * @param {String} key
   * @param {Object?} data Optional entity data
   */
  constructor(library, key, data = null) {
    // validation
    ow(library, ow.object.instanceOf(Library)); // todo: everywhere or remove
    ow(key, ow.string.nonEmpty);
    ow(data, ow.any(ow.object, ow.null));
    // init
    this.library = library;
    this.key = key;
    this.__data = data;
    this.version = data ? data.version : null;
  }

  /**
   * Returns item data, fetching it from the server if necessary.
   * @param {string|null?} property Property to get
   * @param {Boolean?} update If true, update existing data from the server
   * @returns {Promise<Object|String|Array>}
   */
  async data(property, update= false) {
    if (update || this.__data === null) {
      await this.fetch();
    }
    return property ? this.__data[property] : this.__data;
  }

  /**
   * Fetches the data from the server and returns the instance for method
   * chaining
   *
   * @return {Promise<Item|Collection|Attachment|Entity>}
   */
  async fetch() {
    let opts = {
      retry: this.library.server.retryAttempts
    };
    let response = await this.library._api().items(this.key).get(opts);
    this.__data = response.getData();
    this.version = this.__data.version;
    return this;
  }

  /**
   * internal helper function to assert that the item data has been fetched from the server
   */
  _checkDataFetched(){
    if (this.__data === null) {
      throw new Error("Data needs to be fetched from the server first.");
    }
  }

  /**
   * Synchronously returns a property, which works only after the
   * data has been fetched with `data()`.
   * @param {String} property
   */
  get(property) {
    this._checkDataFetched();
    if (this.__data[property] === undefined) {
      throw new Error(`Property '${property}' does not exist.`);
    }
    return this.__data[property];
  }

  /**
   * Synchronously sets a property, which works only after the
   * data has been fetched with `data()`.
   * @param {String} property
   * @param {String} value
   * @return {Entity}
   */
  set(property, value) {
    this._checkDataFetched();
    if (this.__data[property] === undefined) {
      throw new Error(`Property '${property}' does not exist.`);
    }
    this.__data[property] = value;
    return this;
  }

  /**
   * Save the item
   * @return {Promise<Entity>}
   */
  async save() {
    this._checkDataFetched();
    let opts = {
      ifUnmodifiedSinceVersion: this.version,
      retry: this.library.server.retryAttempts
    };
    let response = await this.library._api().items(this.key).put(this.__data, opts);
    // returns SingleWriteResponse
    this.__data = response.getData();
    this.version = this.__data.version;
    return this;
  }
}

/**
 * A "normal" reference item which can have child items
 */
class Item extends Entity {
  /**
   * Create a new item in the given library
   * @param {Library} library
   * @param {Object} data An map that must contain either valid Zotero item data, or only the `itemType` property,
   * in which case the rest of the fields are added from the item type template
   * @return {Item}
   */
  static async create(library, data) {
    if (!data || !data.itemType) {
      throw new Error("Data must at least contain the itemType property");
    }
    if (Object.keys(data).length === 1) {
      data = await this.library.server.getTemplate(data.itemType);
    }
    let opts = {
      zoteroWriteToken: library.server.createWriteToken(),
      retry: library.server.retryAttempts
    };
    let response = await library._api().items() .post([data], opts);
    if (response.isSuccess()) {
      data = response.getData()[0];
      return new Item(library, data.key, data);
    }
    throw new Error(JSON.stringify(response.getErrors(), null, 2));
  }

  /**
   * Returns the child item instances with the data preloaded.
   * @returns {Promise<Attachment[]|Entity[]>}
   */
  async children() {
    let opts = {
      retry: this.library.server.retryAttempts
    };
    let response = await this.library._api().items(this.key).children().get(opts);
    return response
      .getData()
      .map(item => {
        switch (item.itemType) {
          case "attachment":
            return new Attachment(this.library, item.key, item);
          default:
            return new Entity(this.library, item.key, item);
        }
      });
  }

  /**
   * Returns the parent item instance or null if the item has no parent
   * @returns {Promise<Item>|null}
   */
  async parentItem() {
    let parentItemKey = (await this.data()).parentItem;
    if (parentItemKey) {
      return new Item(this.library, parentItemKey);
    }
    return null;
  }

  /**
   * Returns the collections objects for each collection the item is part of, or an empty array if it is in no
   * collection.
   * @returns {Promise<Collection[]>}
   */
  async collections() {
    let collectionKeys = await this.data("collections");
    let collections = [];
    for (let key in collectionKeys) {
      collections.push(await this.library.collection(key).fetch());
    }
    return collections;
  }

  /**
   * Adds a tag.
   * This is a synchronous operation which does not save the modified item to the server.
   * @param {String} tag
   * @return {Item}
   */
  addTag(tag) {
    this._checkDataFetched();
    // ignore duplicates
    if (!this.hasTag(tag)) {
      this.__data['tags'].push({
        tag,
        type: 1
      });
    }
    return this;
  }

  /**
   * Returns true if the item has a tag with that name
   * @param {string} tag
   */
  hasTag(tag) {
    this._checkDataFetched();
    return this.__data['tags'].some(t => t.tag === tag);
  }

  /**
   * Adds a relation.
   * This is a synchronous operation which does not save the modified item to the server.
   * @param {string} type
   * @param {string} uri
   * @return {Item}
   */
  addRelation(type, uri) {
    if (!this.__data.relations){
      this.__data.relations = {};
    }
    if (!this.__data.relations[type]) {
      this.__data.relations[type] = [];
    } else if (typeof this.__data.relations[type] === "string") {
      this.__data.relations[type] = [this.__data.relations[type]];
    }
    this.__data.relations[type].push(uri);
    return this;
  }

  /**
   * Move the item to the given item or collection within the same library.
   * @param {Item|Collection} target
   * @returns {Promise<Item>}
   */
  async moveTo(target) {
    if (target.library.id !== this.library.id) {
      throw new Error("Cannot move entity to different library. Use copyTo() and removeFrom() instead.")
    }
    if (target instanceof Item) {
      if (target instanceof Attachment) {
        throw new Error("Cannot move item to attachment");
      }
      if (!(this instanceof Attachment)) {
        throw new Error("Can only move attachments to items")
      }
      return this.set("parentItem", target.key).save();
    } else if (target instanceof Collection) {
      throw new Error("Cannot move an item to a collection. Use copyTo() and removeFrom()");
    }
    throw new Error("Invalid target");
  }

  /**
   * Copies the item to the given item or collection, which can be in a different library.
   * @param {Item|Collection} target
   * @returns {Promise<Item>} The present item if copied to a collection within the same library,
   * otherwise the copied item in the other library or in the other parent item;
   */
  async copyTo(target) {
    if (target instanceof Item) {
      if (target instanceof Attachment) {
        throw new Error("Cannot copy item to attachment");
      }
      if (!(this instanceof Attachment)) {
        throw new Error("Can only copy attachments to items")
      }
      let newItem = await Item.create(target.library, await this.data(null, true));
      return newItem.set("parentItem", targt.key).save();
    } else if (target instanceof Collection) {
      if (target.library.id !== this.library.id) {
        let newItem = await Item.create(target.library, await this.data(null, true));
        return newItem.set("collections", [target.key]).save();
      } else {
        let collections = this.get("collections");
        if (!collections.includes(target.key)) {
          collections.push(target.key); // add array item in-place, no need to set the property again
          await this.save();
        }
        return this;
      }
    }
    throw new Error("Invalid target");
  }

  /**
   * Removes the item from the given item or collection
   * @param {Item|Collection} target
   * @returns {Promise<Item>}
   */
  async removeFrom(target) {
    if (target instanceof Item) {
      if (this.get("parentItem") !== target.key) {
        throw new Error("Item is not a child of the given item")
      }
      return this.set("parentItem", null).save();
    } else if (target instanceof Collection) {
      let collections = await this.data("collections", true);
      let i = collections.indexOf(target.key);
      if (i < 0){
        throw new Error("Item is not in the given collection.");
      }
      collections.splice(i,1); // remove array item in-place, no need to set the property again
      return this.save();
    }
    throw new Error("Invalid target");
  }
}

/**
 * Model for a Zotero attachment item with utility methods for upload and download of the
 * attachment content
 */
class Attachment extends Item {

  /**
   * Returns the url of the file download
   * @returns {Promise<String>}
   */
  async downloadUrl() {
    let opts = {
      retry: this.library.server.retryAttempts
    };
    const libraryApi = this.library._api();
    let response = await libraryApi.items(this.key).attachmentUrl().get(opts);
    return response.raw.trim();
  }

  /**
   * Uploads an attachment
   * @see https://gist.github.com/tnajdek/b2392ee558314aa0d559475b3ff3cf71
   * @param {string} filePath
   * @param {object} options {
   *  handleDuplicateFilename: "throw|replace|ignore",
   *  tags?: String[]
   * }
   * @returns {Promise<void>}
   */
  async upload(filePath, options={}) {
    let currStep;
    let tags = [];
    if (options.tags) {
      if (!Array.isArray(options.tags)) {
        throw new Error("options.tags must be of type array");
      }
      tags = options.tags.map(tag => ({tag}));
    }
    const libraryApi = this.library._api();
    if (!("handleDuplicateFilename" in options)) {
      options.handleDuplicateFilename = "throw";
    }
    // check if attachment already exists
    let filename = path.basename(filePath);
    try {
      currStep = "looking up parent item";
      let parentItem = await this.parentItem();
      if (!parentItem) {
        throw new Error(`Could not find parent item for attachment ${filePath}`);
      }
      const buffer = await fs.promises.readFile(filePath);
      const attachment =(await parentItem.children())
        .find(attachment => attachment.get("filename") === filename);
      if (attachment) {
        currStep = "replacing attachment";
        const md5sum = attachment.get('md5');
        // upload file
        await libraryApi
          .items(attachment.key)
          .attachment(filename, buffer, null, md5sum)
          .post();
        // update item if necessary
        if (tags) {
          let opts = {
            retry: this.library.server.retryAttempts
          };
          const attachmentItem = (await libraryApi.items(attachment.key).get(opts)).getData();
          await libraryApi
            .items(attachment.key)
            .patch({tags}, {
              ifUnmodifiedSinceVersion:attachmentItem.version,
              retry: this.library.server.retryAttempts
            });
        }
      } else {
        currStep = "getting attachment template";
        let opts = {
          linkMode: 'imported_file',
          retry: this.library.server.retryAttempts
        };
        const template = (await this.library.server._api().template('attachment').get(opts)).getData();
        currStep = "uploading new attachment item";
        const item = {
          ...template,
          title: filename,
          parentItem: parentItem.key,
          filename,
          contentType: mime.lookup(filePath),
          tags
        };
        const attachmentItem =
          (await libraryApi
            .items()
            .post([item]))
              .getEntityByIndex(0);
        currStep = "uploading file";
        try {
          await libraryApi
            .items(attachmentItem.key)
            .attachment(filename, buffer)
            .post();
        } catch (e) {
          currStep = "deleting item after failed upload";
          await libraryApi
            .items()
            .delete([attachmentItem.key],{version:attachmentItem.version});
          throw e;
        }
      }
    } catch(e) {
      let message = e.message;
      if (typeof e.reason == "string") {
        message = `${e.message}\nWhen ${currStep}, the following error occurred: ${e.reason}`;
      }
      throw new Error(message);
    }
  }
}

/**
 * Model for a Zotero collection
 */
class Collection extends Entity {

  /**
   * Create a new collection in the given library unless a collection with that name already
   * exists
   *
   * @param {Library} library
   * @param {String} name
   * @param {Collection|String|Boolean?} parentCollection If false, create at the top level
   * @param {Boolean?} throwIfExists Throw an error if collection with that name exists
   * @return {Collection} The newly created or (if throwIfExists === false) àny existing collection
   * @override
   */
  static async create(library, name, parentCollection=false, throwIfExists=false) {
    let api = library._api();
    let response;
    let opts = {
      q: name,
      retry: library.server.retryAttempts
    };
    if (parentCollection) {
      let parentKey = parentCollection;
      if (parentCollection instanceof Collection) {
        parentKey = parentCollection.key;
      } else if (typeof parentCollection !== "string" ) {
        throw new Error("parentCollection argument must be either a string (the key) or an instance of Collection");
      }
      response = await api.collections(parentKey).subcollections().get(opts);
    } else {
      response = await api.collections().top().get(opts);
    }
    if (response.getData().length > 0) {
      let collection = response.getData().find(coll => coll.name === name );
      if (collection) {
        if (throwIfExists) {
          throw new Error(`A collection with the name "${name}" already exists.`);
        }
        return new Collection(library, collection.key, collection);
      }
    }
    let collectionData = [{
      name,
      parentCollection
    }];
    opts = {
      retry: library.server.retryAttempts
    };
    response = await api.collections().post(collectionData, opts);
    if (response.isSuccess()) {
      let data = response.getData();
      return new Collection(library, data[0].key, data[0]);
    }
    throw new Error(JSON.stringify(response.getErrors(), null, 2));
  }

  /**
   * Returns the collection by the given collection path, or false if no collection exists with this path.
   * Note: currently, only the first 100 top collections are supported.
   * @param {Library} library
   * @param {string} collectionPath
   * @param {string?} separator Optional separator, defaults to "/"
   * @returns {Promise<boolean|Collection>}
   */
  static async byPath(library, collectionPath, separator="/") {
    if (! library instanceof Library) {
      throw new Error("First argument must be an instance of Library");
    }
    let parts = collectionPath.split(separator);
    let collection;
    for (let part of parts) {
      if (!part) continue;
      if (!collection) {
        // this works only for the first 100 top folders
        let opts = {
          retry: library.server.retryAttempts,
          limit: 100
        };
        let response = await library._api().collections().top().get(opts);
        let data = response.getData().find(data => data.name === part);
        if (!data) {
          return false;
        }
        collection = new Collection(library, data.key, data);
      } else {
        let found = false;
        for await (let coll of collection.subcollections()) {
          if (coll.get("name") === part) {
            found = coll;
            break;
          }
        }
        if (! found){
          return  false;
        }
        collection = found;
      }
    } // for of
    return collection;
  }

  /**
   * Returns an async iterator that provides {@link Collection} instances for each subcollection in this collection.
   * Iterate over result with `for await (let item of collection.subcollections())`
   * @param {Number?} limit Optional batch size limit, defaults to 100 (maximum number)
   * @returns {AsyncGenerator<Collection>}
   */
  async * subcollections(limit=100) {
    if (limit > 100) {
      throw new Error("Limit cannot be greater than 100");
    }
    let opts = {
      retry: this.library.server.retryAttempts,
      limit
    };
    let totalSubCollections;
    let counter = 1;
    do {
      opts.start = counter;
      debug(`Fetching ${limit} collection records from server`);
      let response = await this.library._api().collections(this.key).subcollections().get(opts);
      let collections = response.getData().map(data => new Collection(this.library, data.key, data));
      if (!totalSubCollections) {
        totalSubCollections = Number(response.response.headers.get("total-results"));
        debug(`${totalSubCollections} collection total`);
      }
      for (let collection of collections) {
        yield collection;
      }
      counter += limit;
    } while (counter <= totalSubCollections);
  }


  /**
   * Returns an async iterator that provides {@link Item} instances for each item in this collection.
   * Iterate over result with `for await (let item of collection.items())`
   * @param {string} query If given, filter the items with the query
   * @param {Number?} limit Optional batch size limit, defaults to 100 (maximum number)
   * @returns {AsyncGenerator<Item>}
   */
  async * items(query=null, limit=100) {
    if (limit > 100) {
      throw new Error("Limit cannot be greater than 100");
    }
    let opts = {
      retry: this.library.server.retryAttempts,
      limit
    };
    if (query) {
      opts.q = query;
    }
    let totalItems;
    let counter = 1;
    do {
      opts.start = counter;
      debug(`Fetching ${limit} item records from server`);
      let response = await this.library._api().collections(this.key).items().get(opts);
      let items = response.getData().map(data => new Item(this.library, data.key, data));
      if (!totalItems) {
        totalItems = Number(response.response.headers.get("total-results"));
        debug(`${totalItems} items total`);
      }
      for (let item of items) {
        yield item;
      }
      counter += limit;
    } while (counter <= totalItems);
  }

  /**
   * Returns the number of items contained in the collection
   * @returns {Promise<Number>}
   */
  async size() {
    let opts = {
      retry: this.library.server.retryAttempts,
      limit: 1
    };
    let response = await this.library._api().collections(this.key).items().get(opts);
    return Number(response.response.headers.get("total-results"));
  }

  /**
   * Returns the instance representing this collection's parent collection or false if it
   * doesn't have a parent
   * @returns {Promise<Collection|boolean>}
   */
  async parentCollection() {
    let parentKey = await this.data("parentCollection");
    if (!parentKey) {
      // collection has no parent
      return false;
    }
    return await this.library.collection(parentKey).fetch();
  }

  /**
   * Returns the path of this collection within the collection tree
   * @param {string} separator The string used to separate the collection names in the path, defaults to "/"
   * @returns {Promise<string>}
   */
  async path(separator="/") {
    let parts = [];
    let current = this;
    do {
      parts.push(await current.data("name"));
      current = await current.parentCollection();
    } while (current)
    return parts.join(separator);
  }

  /**
   * Moves the collection under the given parent collection
   * @param {Collection} parent
   * @returns {Promise<Collection>}
   */
  async moveTo(parent) {
    if (! parent instanceof Collection) {
      throw new Error("Argument must be an instance of Collection");
    }
    if (parent.library !== this.library) {
      throw new Error("Collections can only be moved within the same library");
    }
    if (parent === this) {
      throw new Error("Cannot move collection to itself.");
    }
    let ancestor = await parent.parentCollection();
    while (ancestor) {
      if (ancestor.key === this.key) {
        throw new Error("Cannot move a collection to one of its subcollections")
      }
      ancestor = await parent.parentCollection();
    }
    this.set('parentCollection', parent.key);
    await this.save();
    return this;
  }
}

module.exports = {
  Server,
  Library,
  Entity,
  Item,
  Attachment,
  Collection
};
