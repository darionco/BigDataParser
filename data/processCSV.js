const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');

function processCSV(file, options, cb) {
    const csvOptions = Object.assign({}, options);
    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(path.resolve(__dirname, file))
            .pipe(csv(csvOptions))
            .on('data', data => {
                cb(data);
            }).on('end', () => {
                stream.destroy();
            }).on('error', e => {
                reject(e);
            }).on('close', () => {
                resolve();
            });
    });
}

module.exports = processCSV;
