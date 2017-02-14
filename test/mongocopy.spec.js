const mongocopy = require('..');
const {MongoClient} = require('mongodb');
const async = require('async');
const {expect} = require('chai');

const testData = {
	products: [
		{userId: 1, _id: 1, name: 'apple'},
		{userId: 1, _id: 2, name: 'orange'}
	],
	customers: [
		{userId: 1, _id: 1, name: 'bob'},
		{userId: 1, _id: 2, name: 'rob'},
		{userId: 2, _id: 3, name: 'li'}
	],
	countries: [
		{_id: 1, code: 'UA'},
		{_id: 2, code: 'ES'},
		{_id: 3, code: 'DK'}
	]
};

const testConfig = {
	dbFrom: {uri: 'mongodb://localhost/xprod'},
	dbTo: {uri: 'mongodb://localhost/xstage'},
	data: {
		products: {
			query: {userId: 1},
			transform: function(doc, cb) {
				setTimeout(function() {
					doc.name += ' xxl';
					return cb(null, doc);
				}, 1);
			}
		},
		customers: {
			query: {userId: 1},
			transform: function(doc, cb) {
				doc.name = 'mr ' + doc.name;
				return cb(null, doc);
			}
		},
		countries: {}
	},
	log: true
};

let prod, stage;

function prepareDbs(done) {
	async.series([
		(cb) => prod.dropDatabase(cb),
		(cb) => stage.dropDatabase(cb),
		(cb) => prod.collection('products').insertMany(testData.products, cb),
		(cb) => prod.collection('customers').insertMany(testData.customers, cb),
		(cb) => prod.collection('countries').insertMany(testData.countries, cb),
	], done);
}

function connectToDbs(done) {
	async.series([
		(cb) => {
			MongoClient.connect(testConfig.dbFrom.uri, (err, db) => {
				prod = db;
				cb(err);
			});
		},
		(cb) => {
			MongoClient.connect(testConfig.dbTo.uri, (err, db) => {
				stage = db;
				cb(err);
			});
		},
	], done);
}

describe('mongocopy', function() {

	before('connect to dbs', connectToDbs);

	describe('run mongocopy in execute mode', () => {

		before('prepare dbs', prepareDbs);

		it('should copy data', (done) => {
			mongocopy(testConfig, (err, report) => {
				if (err) {
					return done(err);
				}

				expect(report).to.be.eql({
					products: {copied: 2},
					customers: {copied: 2},
					countries: {copied: 3}
				});
				done();
			});
		});

		it('should read new records of products', (done) => {
			stage.collection('products').find({}).toArray(function(err, docs){
				if(err) {
					return done(err);
				}
				//'returns copied and transformed records for products'
				expect(docs).to.be.eql([
					{userId: 1, _id: 1, name: 'apple xxl'},
					{userId: 1, _id: 2, name: 'orange xxl'}
				]);
				done();
			});
		});

		it('returns copied and transformed records for customers', (done) => {
			stage.collection('customers').find({}).toArray(function(err, docs){
				if(err) {
					return done(err);
				}
				expect(docs).to.be.eql([
					{userId: 1, _id: 1, name: 'mr bob'},
					{userId: 1, _id: 2, name: 'mr rob'},
				]);
				done();
			});
		});

		it('returns all copied countries', (done) => {
			stage.collection('countries').find({}).toArray(function(err, docs){
				if(err) {
					return done(err);
				}
				expect(docs).to.be.eql(testData.countries);
				done();
			});
		});

		it('should copy same data from xprod to xstage with duplicates ignored', done => {
			const ignoreDuplicates = Object.assign({}, testConfig, {ignoreDuplicates: true});
			mongocopy(ignoreDuplicates, function(err, report) {
				if(err) {
					return done(err);
				}
				//'reports that nothing was copied because of duplicates'
				expect(report).to.be.eql({
					products: {copied: 0, duplicates: 2, duplicateIds: [1, 2]},
					customers: {copied: 0, duplicates: 2,  duplicateIds: [1, 2]},
					countries: {copied: 0, duplicates: 3,  duplicateIds: [1, 2, 3]}
				});
				done();
			});
		});

		it('copying same data from xprod to xstage without duplicates ignored', done => {
			mongocopy(testConfig, (err, report) => {
				//'error when copying from xprod to xstage'
				expect(err).to.be.ok;
				//'reports that nothing was copied because of the first duplicate occured'
				expect(report).to.be.eql({
					products: {copied: 0, duplicates: 1, duplicateIds: [1]}
				});
				done();
			});
		});
	});

	describe('dry run', () => {
		before('prepare dbs', prepareDbs);

		it('should do dry run', done => {
			const dryRun = Object.assign({}, testConfig, {dryRun: true});
			mongocopy(dryRun, function(err, report) {
				if(err) {
					return done(err);
				}
				//'reports documents satisfying the given queries'
				expect(report).to.deep.eql({
					countries: {copied: 3},
					customers: {copied: 2},
					products: {copied: 2}
				});
				done();
			});
		});
		['products', 'customers', 'countries'].forEach((collection) => {
			it(`should not not adding any new documents to xstage ${collection}`, done => {
				stage.collection(collection).find({}).toArray(function(err, docs){
					if(err) {
						return done(err);
					}
					expect(docs).to.deep.eql([]);
					done();
				});
			});
		});
	});
});