const { MongoClient } = require('mongodb');
const stream = require('stream');
const async = require('async');

const DUPLICATE_KEY_ERROR = 11000;

function run(opts, cb) {
	const collections = Object.keys(opts.data);
	let {dbFrom, dbTo} = opts;
	if(dbFrom.ObjectId && dbTo.ObjectId) {
		return startCopy();
	}

	const report = {};

	async.parallel({
		dbFrom: (done) => MongoClient.connect(dbFrom.uri, dbFrom.options, done),
		dbTo: (done) => MongoClient.connect(dbTo.uri, dbTo.options, done)
	}, (err, dbs) => {
		if(err) {
			return cb(err);
		}
		dbFrom = dbs.dbFrom.db();
		dbTo = dbs.dbTo.db();
		startCopy();
	});

	function startCopy() {
		log('copying..');
		return async.eachSeries(collections, runOne, function(err) {
			log('finished.');
			return cb(err, report);
		});
	}


	function runOne(colName, cb) {
		const colFrom = dbFrom.collection(colName);
		const colTo = dbTo.collection(colName);
		const query = opts.data[colName].query || {};
		const {transform} = opts.data[colName];

		const insy =  new stream.Transform({objectMode: true});
		insy._transform = function (doc, enc, cb) {
			if (opts.dryRun) {
				this.push(doc);
				return cb(null);
			}
			const _this = this;
			colTo.insertOne(doc, function(err, newDoc) {
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
		};

		log(colName, 'started..');
		let cursor = colFrom.find(query);
		report[colName] = {copied: 0, duplicates: 0, duplicateIds: []};

		if (transform) {
			const transy = new stream.Transform({objectMode: true});
			transy._transform = function(doc, enc, cb) {
				const _this = this;
				return transform(doc, function(err, newDoc) {
					if (err){
						return cb(err);
					}
					newDoc && _this.push(newDoc);
					return cb();
				});
			};
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
