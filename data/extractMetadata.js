const fs = require('fs');
const path = require('path');
const processCSV = require('./processCSV');
const {TextEncoder} = require('util');

const intTypes = {
    signed: [
        {
            type: 'Int8',
            size: 1,
            min: -128,
            max: 127,
        },
        {
            type: 'Int16',
            size: 2,
            min: -32768,
            max: 32767,
        },
        {
            type: 'Int32',
            size: 4,
            min: -2147483648,
            max: 2147483647,
        },
    ],
    unsigned: [
        {
            type: 'Uint8',
            size: 1,
            min: 0,
            max: 255,
        },
        {
            type: 'Uint16',
            size: 2,
            min: 0,
            max: 65535,
        },
        {
            type: 'Uint32',
            size: 4,
            min: 0,
            max: 4294967295,
        },
    ],
};

const funtionTable = {
    'Int8': 'writeInt8',
    'Int16': 'writeInt16LE',
    'Int32': 'writeInt32LE',
    'Uint8': 'writeUInt8',
    'Uint16': 'writeUInt16LE',
    'Uint32': 'writeUInt32LE',
    'Float32': 'writeFloatLE',
};

async function main() {
    const meta = {
        columns: {
            'Origin_airport': {
                type: 'string',
                max: 0,
            },
            'Destination_airport': {
                type: 'string',
                max: 0,
            },
            'Origin_city': {
                type: 'string',
                max: 0,
            },
            'Destination_city': {
                type: 'string',
                max: 0,
            },
            'Passengers': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
            },
            'Seats': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
            },
            'Flights': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
            },
            'Distance': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
            },
            'Fly_date': {
                type: 'date',
                max: 0,
            },
            'Origin_population': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
            },
            'Destination_population': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
            },
            'Org_airport_lat': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
            },
            'Org_airport_long': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
            },
            'Dest_airport_lat': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
            },
            'Dest_airport_long': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
            },
        },
        countRaw: 0,
        count: 0,
        rowSize: 0,
    };

    let start;
    let end;

    const keys = Object.keys(meta.columns);
    const encoder = new TextEncoder();

    start = new Date();
    await processCSV('./Airports2.csv', null, data => {
        let add = true;
        keys.forEach(key => {
            if (meta.columns[key].type === 'number') {
                if (isNaN(data[key])) {
                    add = false;
                } else {
                    const v = parseFloat(data[key]);
                    meta.columns[key].isFloat = meta.columns[key].isFloat || (v % 1 !== 0);
                    meta.columns[key].min = Math.min(meta.columns[key].min, v);
                    meta.columns[key].max = Math.max(meta.columns[key].max, v);
                }
            } else {
                const s = encoder.encode(data[key]);
                meta.columns[key].max = Math.max(meta.columns[key].max, s.length);
            }
        });

        if (add) {
            meta.count += 1;
        }
        meta.countRaw += 1;
    });
    end = new Date();
    console.log(end - start);

    const finalMeta = {
        columns: {},
        columnOrder: keys,
        count: meta.count,
        rowSize: 0,
    };
    keys.forEach(key => {
        if (meta.columns[key].type === 'number') {
            if (meta.columns[key].isFloat) {
                finalMeta.columns[key] = {
                    type: 'Float32',
                    size: 4,
                };
            } else {
                let types;
                if (meta.columns[key].min < 0) {
                    types = intTypes.signed;
                } else {
                    types = intTypes.unsigned;
                }

                for (let i = 0, n = types.length; i < n; ++i) {
                    if (meta.columns[key].min >= types[i].min && meta.columns[key].max <= types[i].max) {
                        finalMeta.columns[key] = {
                            type: types[i].type,
                            size: types[i].size,
                        };
                        break;
                    }
                }
            }
        } else {
            finalMeta.columns[key] = {
                type: meta.columns[key].type,
                size: meta.columns[key].max,
            };
        }

        finalMeta.rowSize += finalMeta.columns[key].size;
    });

    const header = encoder.encode(JSON.stringify(finalMeta));
    const result = Buffer.alloc(finalMeta.rowSize * finalMeta.count + header.length + 4);
    const rowBuffer = Buffer.alloc(finalMeta.rowSize);
    let offset = 0;

    result.writeUInt32LE(header.length, 0);
    offset += 4;

    for (let i = 0, n = header.length; i < n; ++i) {
        result.writeUInt8(header[i], offset + i);
    }
    offset += header.length;

    start = new Date();
    await processCSV('./Airports2.csv', null, data => {
        let add = true;
        let rowOffset = 0;
        rowBuffer.fill(0);
        keys.forEach(key => {
            if (meta.columns[key].type === 'number') {
                if (isNaN(data[key])) {
                    add = false;
                } else {
                    const v = parseFloat(data[key]);
                    rowBuffer[funtionTable[finalMeta.columns[key].type]](v, rowOffset);
                    rowOffset += finalMeta.columns[key].size;
                }
            } else {
                const s = encoder.encode(data[key]);
                for (let i = 0, n = s.length; i < n; ++i) {
                    rowBuffer.writeUInt8(s[i], rowOffset + i);
                }
                rowOffset += finalMeta.columns[key].size;
            }
        });

        if (add) {
            for (let i = 0, n = rowBuffer.length; i < n; ++i) {
                result.writeUInt8(rowBuffer.readUInt8(i), offset + i);
            }
            offset += finalMeta.rowSize;
        }
    });
    end = new Date();
    console.log(end - start);

    let output = path.resolve(__dirname, './metadata.json');
    fs.writeFile(output, JSON.stringify(finalMeta), err => {
        if (err) {
            return console.log(err);
        }
        console.log('Created ' + output);

        output = path.resolve(__dirname, './data.bin');
        fs.writeFile(output, result, err => {
            if (err) {
                return console.log(err);
            }
            console.log('Created ' + output);
        });
    });
}

main();
