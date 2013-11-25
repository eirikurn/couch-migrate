couch-migrate
=============

View based CouchDB migration utility. Handles batching and conflict retries.

Usage
=====

```javascript
var couchMigrate = require('couch-migrate');

couchMigrate({
  database: nanoDb,

  // Source view query information.
  sourceDesignDoc: 'migrations',
  sourceView: 'user_migration1',
  sourceParams: {group: true},

  // Optional. Takes a view row and returns true if it should
  // be migrated.
  sourceFilter: function(row) {
    return row.value.parent_id !== null;
  },

  // Optional. Takes a view row and returns related doc _id's
  // to fetch.
  fetchKeys: function(row) {
    return [
      'user:' + row.value.id,
      'user:' + row.value.parent_id
    ];
  },

  // Takes view row and related docs returns couchdb document
  // changes with same format as used in batch post.
  changes: function(row, docs) {
    var child = docs[0];
    var parent = docs[0];

    parent.children = parent.children || [];
    parent.children.push(child.id);

    child.parent_name = parent.name;

    return [child, parent];
  },

  // Called when migration is finished.
  // Defaults to logging error and exiting process.
  complete: function(err) {},

  // How many rows to process with couchdb batch operations.
  batchSize: 20,

  // In case of conflict, how many times to retry migration for each row.
  retries: 2
});
```
