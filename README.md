mongocopy [![Build Status](https://travis-ci.org/bubenshchykov/mongocopy.png?branch=master)](https://travis-ci.org/bubenshchykov/mongocopy)
=========

Sometimes we need to copy a special set of user data from one environment to another.
The module fits nicely when you want to
* select a set of collections
* add match criteria to the documents inside
* specify transform function for documents
* and get it on other db

example
====

Assume you have production and staging database.
In production db you have
```javascript
{
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
	],
	files: [..],
	other: [..]
}
```

You want to copy only a few collections for the user 1, transform the documents inside because of env differences, and also you want some dictionary eg countries

```javascript
var mongocopy = require('mongocopy');

var opts = {
		uriFrom: 'mongodb://localhost:27017/production',
		uriTo: 'mongodb://localhost:27017/staging',
		data: {
			products: {
				query: {userId: 1},
				transform: function(doc, cb) {
					doc.name += ' xxl';
					return cb(null, doc);
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
		dryRun: true, // fake run without inserts, reports docs which are found by given queries
		log: true, // streaming logs, false by default
		ignoreDuplicates: true // continue if duplicate error occured, false by default
	}
};

mongocopy(opts, function(err, report) {
	console.log(arguments);
});

```

That's it!
