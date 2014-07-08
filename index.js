var mongojs = require('mongojs');
var stream = require('stream');
var async = require('async');

function run(opts, cb) {
	if (!(opts && opts.uriFrom && opts.uriTo && opts.data)) {
		throw 'uriFrom|uritTo|data options are missing';
	}

	var collections = Object.keys(opts.data);
	var dbFrom = mongojs(opts.uriFrom, collections);
	var dbTo = mongojs(opts.uriTo, collections);

	return async.eachSeries(collections, runOne, cb);

	function runOne(colName, cb) {
		var colFrom = dbFrom[colName];
		var colTo = dbTo[colName];
		var query = opts.data[colName].query || {};
		var transform = opts.data[colName].transform;

		var insy =  new stream.Writable({objectMode: true});
		insy._write = function (doc, enc, cb) {
			return colTo.insert(doc, cb);
		}

		var res = colFrom.find(query);

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
			res = res.pipe(transy).pipe(insy);
		} else {
			res = res.pipe(insy);
		}
		
		return res
			.on('error', function(err) {
				return cb(err);
			})
			.on('finish', function() {
				return cb();
			});
	}
}

module.exports = run;