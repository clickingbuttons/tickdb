const http = require('http');
const fs = require('fs');
const path = require('path');

const fname = path.join(__dirname, "./query.js");
const query = fs.readFileSync(fname, 'utf8');
const post_data = JSON.stringify({
	table: 'agg1d0',
	from: '1970-01-01',
	to:   '1970-01-02',
	source: {
		text: query,
		path: fname
	}
});
const post_options = {
	host: '127.0.0.1',
	port: '8080',
	path: '',
	method: 'POST',
	headers: {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(post_data)
	},
};

const post_req = http.request(post_options, res => {
	res.setEncoding('utf8');
	res.on('data', function (chunk) {
		console.log(chunk);
	});
});
post_req.write(post_data);
post_req.end();

