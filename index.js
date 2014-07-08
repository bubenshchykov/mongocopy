var mongojs = require('mongojs');
var stream = require('stream');
var async = require('async');

var DUPLICATE_KEY_ERROR = 11000;

function run(opts, cb) {
	if (!(opts && opts.uriFrom && opts.uriTo && opts.data)) {
		throw 'uriFrom|uritTo|data options are missing';
	}

	var collections = Object.keys(opts.data);
	var dbFrom = mongojs(opts.uriFrom, collections);
	var dbTo = mongojs(opts.uriTo, collections);

	var report = {};
	log('copying..');
	return async.eachSeries(collections, runOne, function(err) {
		log('finished.');
		return cb(err, report);
	});

	function runOne(colName, cb) {
		var colFrom = dbFrom[colName];
		var colTo = dbTo[colName];
		var query = opts.data[colName].query || {};
		var transform = opts.data[colName].transform;

		var insy =  new stream.Transform({objectMode: true});
		insy._transform = function (doc, enc, cb) {
			var _this = this;
			return colTo.insert(doc, function(err, newDoc) {
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

		log(colName, query, 'started..');
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
					_this.push(newDoc);
					return cb();
				});
			}
			cursor = cursor.pipe(transy).pipe(insy);
		} else {
			cursor = cursor.pipe(insy);
		}
		
		return cursor
			.on('data', function(data) {
				report[colName].copied++;
			})
			.on('error', cb)
			.on('finish', function() {
				!report[colName].duplicates && delete report[colName].duplicates;
				!report[colName].duplicateIds.length && delete report[colName].duplicateIds;
				log(colName, query, 'finished, docs copied:', report[colName].copied);
				return cb();
			});
	}

	function log() {
		opts.log && console.log.apply(this, ['	'].concat([].slice.call(arguments)));
	}
}

module.exports = run;