var mongocopy = require('./');
var mongojs = require('mongojs');
var test = require('tape');

var testData = {
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

var testConfig = {
		dbFrom: {uri: 'xprod'},
		dbTo: {uri: 'xstage'},
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

test('cleaning previous test data', function(t) {
	var prod = mongojs('xprod', ['products', 'customers', 'countries']);
	prod.dropDatabase(function(err){
		t.notOk(err, 'xprod db removed');
		var stage = mongojs('xstage', ['products', 'customers', 'countries']);
		stage.dropDatabase(function(err){
			t.notOk(err, 'xstage db removed');

			t.test('mocking prod db', function(t) {
				prod.products.insert(testData.products, function(err) {
					t.notOk(err, 'added products to xprod');
					prod.customers.insert(testData.customers, function(err) {
						t.notOk(err, 'added customers to xprod');
						prod.countries.insert(testData.countries, function(err){
							t.notOk(err, 'added countries to xprod');

							t.test('copying data from xprod to xstage', function(t) {
								mongocopy(testConfig, function(err, report) {
									t.notOk(err, 'copied from xprod to xstage');
									t.deepEqual(report, {
										products: {copied: 2},
										customers: {copied: 2},
										countries: {copied: 3}
									}, 'reports an expected result');

									t.test('reading the new records from the xstage', function(t){
										t.plan(6);
										stage.products.find({}).toArray(function(err, docs){
											t.notOk(err, 'found products on xstage');
											t.deepEqual(docs, [
												{userId: 1, _id: 1, name: 'apple xxl'},
												{userId: 1, _id: 2, name: 'orange xxl'}
											], 'returns copied and transformed records for products');
										});
										stage.customers.find({}).toArray(function(err, docs){
											t.notOk(err, 'found customers on xstage');
											t.deepEqual(docs, [
												{userId: 1, _id: 1, name: 'mr bob'},
												{userId: 1, _id: 2, name: 'mr rob'},
											], 'returns copied and transformed records for customers');
										});
										stage.countries.find({}).toArray(function(err, docs){
											t.notOk(err, 'found countries on xstage');
											t.deepEqual(docs, testData.countries, 'returns all copied countries');
										});
									});

									t.test('copying same data from xprod to xstage with duplicates ignored', function(t) {
										var ignoreDuplicates = JSON.parse(JSON.stringify(testConfig));
										ignoreDuplicates.ignoreDuplicates = true;
										mongocopy(ignoreDuplicates, function(err, report) {
											t.notOk(err, 'copied from xprod to xstage');
											t.deepEqual(report, {
												products: {copied: 0, duplicates: 2, duplicateIds: [1, 2]},
												customers: {copied: 0, duplicates: 2,  duplicateIds: [1, 2]},
												countries: {copied: 0, duplicates: 3,  duplicateIds: [1, 2, 3]}
											}, 'reports that nothing was copied because of duplicates');
											t.end();
										});
									});

									t.test('copying same data from xprod to xstage without duplicates ignored', function(t) {
										mongocopy(testConfig, function(err, report) {
											t.ok(err, 'error when copying from xprod to xstage');
											t.deepEqual(report, {
												products: {copied: 0, duplicates: 1, duplicateIds: [1]}
											}, 'reports that nothing was copied because of the first duplicate occured');
											t.end();
										});
									});
								});
							});


							t.end();
						})
					});
				});
			});
			t.end();
		});
	});
});

test('cleaning previous test data', function(t) {
	var prod = mongojs('xprod', ['products', 'customers', 'countries']);
	prod.dropDatabase(function(err){
		t.notOk(err, 'xprod db removed');
		var stage = mongojs('xstage', ['products', 'customers', 'countries']);
		stage.dropDatabase(function(err){
			t.notOk(err, 'xstage db removed');

			t.test('mocking prod db', function(t) {
				prod.products.insert(testData.products, function(err) {
					t.notOk(err, 'added products to xprod');
					prod.customers.insert(testData.customers, function(err) {
						t.notOk(err, 'added customers to xprod');
						prod.countries.insert(testData.countries, function(err){
							t.notOk(err, 'added countries to xprod');

							t.test('copying data from xprod to xstage in dry run', function(t) {
								var dryRun = JSON.parse(JSON.stringify(testConfig));
								dryRun.dryRun = true;
								mongocopy(dryRun, function(err, report) {
									t.notOk(err, 'dry run went good');
									t.deepEqual(report, {
										countries: {copied: 3},
										customers: {copied: 2},
										products: {copied: 2}
									}, 'reports documents satisfying the given queries');
									t.test('reading the new records from the xstage', function(t){
										t.plan(6);
										stage.products.find({}).toArray(function(err, docs){
											t.notOk(err, 'found products on xstage');
											t.deepEqual(docs, [], 'is not adding any new documents to xstage');
										});
										stage.customers.find({}).toArray(function(err, docs){
											t.notOk(err, 'found customers on xstage');
											t.deepEqual(docs, [], 'is not adding any new documents to xstage');
										});
										stage.countries.find({}).toArray(function(err, docs){
											t.notOk(err, 'found countries on xstage');
											t.deepEqual(docs, [], 'is not adding any new documents to xstage');
										});
									});
									t.end();
								});
							});
							t.end();
						})
					});
				});
			});
			t.end();
		});
	});
});

test('end', function(t) {
	t.end();
	process.exit(0);
});