var mongojs = require('mongojs');
var stream = require('stream');
var async = require('async');

var DUPLICATE_KEY_ERROR = 11000;

function run(opts, cb) {
	var collections = Object.keys(opts.data);
	var dbFrom = opts.dbFrom.ObjectId ?
		opts.dbFrom :
		mongojs(opts.dbFrom.uri, opts.dbFrom.options, collections);
	var dbTo = opts.dbTo && opts.dbTo.ObjectId ?
		opts.dbTo :
		mongojs(opts.dbTo.uri, opts.dbTo.options, collections);

	var report = {};
	log('copying..');
	return async.eachSeries(collections, runOne, function(err) {
		log('finished.');
		return cb(err, report);
	});

	function runOne(colName, cb) {
		var colFrom = dbFrom[colName];
		var colTo = dbTo[colName];
		if (!colFrom || !colTo) {
			return cb(new Error('There is no column "' + colName + '" in database'));
		}
		var query = opts.data[colName].query || {};
		var transform = opts.data[colName].transform;

		var insy =  new stream.Transform({objectMode: true});
		insy._transform = function (doc, enc, cb) {
			if (opts.dryRun) {
				this.push(doc);
				return cb(null);
			}
			var _this = this;
			colTo.insert(doc, function(err, newDoc) {
				if (err) {
					if (err.code === DUPLICATE_KEY_ERROR) {
						report[colName].duplicates++;
						report[colName].duplicateIds.push(doc._id);
						return opts.ignoreDuplicates ? cb() : cb(err);
					}
					return cb(err);
				}
				_this.push(newDoc);
				return cb();
			});
		}

		log(colName, 'started..');
		var cursor = colFrom.find(query);
		report[colName] = {copied: 0, duplicates: 0, duplicateIds: []};

		if (transform) {
			var transy = new stream.Transform({objectMode: true});
			transy._transform = function(doc, enc, cb) {
				var _this = this;
				return transform(doc, function(err, newDoc) {
					if (err){
						return cb(err);
					}
					newDoc && _this.push(newDoc);
					return cb();
				});
			}
			cursor = cursor.pipe(transy);
		}

		cursor = cursor.pipe(insy);

		return cursor
			.on('data', function(data) {
				report[colName].copied++;
			})
			.on('error', cb)
			.on('finish', function() {
				!report[colName].duplicates && delete report[colName].duplicates;
				!report[colName].duplicateIds.length && delete report[colName].duplicateIds;
				log(colName, 'finished, docs copied:', report[colName].copied);
				return cb();
			});
	}

	function log() {
		opts.log && console.log.apply(this, ['	'].concat([].slice.call(arguments)));
	}

}

module.exports = run;
