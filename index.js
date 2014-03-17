'use strict';
var async = require('async');
var ProgressBar = require('progress');
var _ = require('lodash');

/**
 * Performs a couchdb migration. Processing a view, batch loading and batch saving
 * changes.
 * @param {nano} options.database Nano database instance.
 * @param {string} options.sourceDesignDoc Design doc to get source data from.
 * @param {string} options.sourceView View name from design doc to get source data from.
 * @param {string} [options.sourceParams={}] Nano params for view query.
 * @param {number} [options.batchSize=20] How large batches to process
 * @param {function} [options.fetchKeys] Callback for specifying extra documents to fetch.
 *                                       Gets one row at a time and returns one or more
 *                                       couchdb keys to fetch.
 * @param {function} [options.changes] Callback for specifying couchdb changes.
 *                                     Gets one row and any extra documents, returns one or more
 *                                     document change (see couchdb bulk api).
 * @param {number} [options.retryConflicts=2] How many retries when changes conflict.
 * @param {function} [options.complete] Callback when the migration is done. Defaults to exiting the process.
 */
module.exports = function(options) {
  var db = options.database;
  var sourceDesignDoc = options.sourceDesignDoc;
  var sourceView = options.sourceView;
  var sourceParams = options.sourceParams || {};
  var batchSize = options.batchSize || 20;
  var limit = options.limit;
  var complete = options.complete || defaultComplete;
  var fetchKeysFn = wrapAsync(options.fetchKeys || function(key, cb) {cb();}, 1);
  var changesFn = wrapAsync(options.changes, 2);
  var retries = options.retryConflicts === false ? 0 : options.retryConflicts || 2;
  var progress = new ProgressBar('Batch :batch, :total rows |:bar| Total :realcurrent rows migrated', {stream: process.stderr, width: 15, total: 1});
  var progressTokens = {realcurrent: 0, batch: 0};

  getViewInBatches(db, sourceDesignDoc, sourceView, sourceParams, 1000, function(rows, nextBatch) {
    progressTokens.batch++;

    if (options.sourceFilter) {
      rows = rows.filter(function(row) {
        try {
          return options.sourceFilter(row);
        } catch(err) {
          reportFailure(row, err);
          return false;
        }
      });
    }

    if (limit != null) {
      limit -= rows.length;
      if (limit < 0) {
        rows = rows.slice(0, rows.length + limit);
        limit = 0;
      }
    }

    // Quickfix: node progress doesn't support a total of 0.
    // I'd prefer it to be fixed there so we can log the batch. 
    if (rows.length === 0) {
      return nextBatch();
    }

    progress.total = rows.length;
    progress.curr = 0;
    progress.tick(0, progressTokens);

    var count = 0;
    async.whilst(function() { return count < rows.length; },
      function(cb) {
        var batch = rows.slice(count, count + batchSize);
        count += batchSize;
        getExtraKeyInfo(batch, cb);
      }, limit === 0 ? complete : nextBatch);
  }, complete);

  function getExtraKeyInfo(batch, cb) {
    async.mapSeries(batch, fetchKeysFn, function(err, keysByRow) {
      if (err) { return cb(err); }
      batch.forEach(function(keys, i) {
        keys.extraKeys = _.flatten([keysByRow[i] || []]);
      });
      processBatch(batch, 0, cb);
    });
  }

  function processBatch(batch, retry, cb) {
    var extraKeys = _.map(batch, 'extraKeys');
    var allExtraKeys = _.flatten(extraKeys);
    var extraDocs = [];
    if (allExtraKeys.length) {
      db.fetch({keys: allExtraKeys}, function(err, body) {
        if (err) { return cb(err); }
        extraDocs = unflatten(_.map(body.rows, 'doc'), _.map(extraKeys, 'length'));
        runChanges();
      });
    } else {
      runChanges();
    }

    /**
     * Gets and bulk applies migration changes.
     */
    function runChanges() {
      async.timesSeries(batch.length, function(i, cb) {
        changesFn(batch[i], extraDocs[i], cb);
      }, function(err, changes) {
        if (err) { return cb(err); }
        changes = changes.map(function(c) { return c || []; });
        var allChanges = _.flatten(changes);

        db.bulk({docs: allChanges}, function(err, results) {
          if (err) {return cb(err); }

          processErrors(unflatten(results, _.map(changes, 'length')));
        });
      });
    }

    /**
     * After running bulk update, check if there are conflicts, if so,
     * we may retry those rows.
     * @param results
     * @returns {*}
     */
    function processErrors(results) {
      var newBatch = [];
      for (var i = 0, r; r = results[i]; i++) {
        if (_.find(r, {error: 'conflict'})) {
          newBatch.push(batch[i]);
        }
      }

      var success = batch.length - newBatch.length;
      progressTokens.realcurrent += success;
      progress.tick(success, progressTokens);

      if (newBatch.length) {
        if (retry >= retries) {
          newBatch.forEach(function(row) { reportFailure(row, 'Conflict'); });
        } else {
          return processBatch(newBatch, retry + 1, cb);
        }
      }

      cb();
    }
  }

  function reportFailure(row, err) {
    console.error('Failed migrating row', row);
    console.error(err);
  }
};


/**
 * Wraps a function so it is async. Only if it doesn't
 * already have a cb argument in the specified position.
 * @param {Function} fn to wrap
 * @param {number} cbArg 0-based seat of callback argument
 * @returns {Function}
 */
function wrapAsync(fn, cbArg) {
  if (!fn) { return fn; }
  if (fn.length > cbArg) {
    return fn;
  }
  return function() {
    var args = Array.prototype.slice.call(arguments, 0);
    var cb = args[cbArg];
    var result;
    try {
      result = fn.apply(null, args);
    } catch(err) {
      return cb(err);
    }
    cb(null, result);
  };
}

/**
 * Unflattens an array to an array of arrays with the specified counts of items from the source array.
 * @param array
 * @param counts
 */
function unflatten(array, counts) {
  var i = 0;
  return counts.map(function(c) {
    var slice = array.slice(i, i + c);
    i += c;
    return slice;
  });
}

/**
 * Loads a couchdb view in batches.
 */
function getViewInBatches(db, ddoc, view, params, batchSize, batchHandler, cb) {
  params.limit = batchSize + 1;

  (function nextBatch() {
    db.view(ddoc, view, params, function(err, body) {
      if (err) { return cb(err); }

      batchHandler(body.rows.slice(0, batchSize), function(err) {
        if (err) { return cb(err); }

        var next = body.rows[batchSize];
        if (next) {
          params.startkey = next.key;
          if (next.id) {
            params.startkey_docid = next.id;
          }
          nextBatch();
        } else {
          cb();
        }
      });
    });
  })();
}

function defaultComplete(err) {
  if (err) {
    console.error('Critical error running migration');
    console.error(err);
  }
  process.exit(err ? 1 : 0);
}
