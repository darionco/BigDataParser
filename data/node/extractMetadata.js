const fs = require('fs');
const pako = require('pako');
const path = require('path');
const processCSV = require('./processCSV');

const intTypes = {
    signed: [
        // {
        //     type: 'Int8',
        //     size: 1,
        //     min: -128,
        //     max: 127,
        // },
        // {
        //     type: 'Int16',
        //     size: 2,
        //     min: -32768,
        //     max: 32767,
        // },
        {
            type: 'Int32',
            size: 4,
            min: -2147483648,
            max: 2147483647,
        },
    ],
    unsigned: [
        // {
        //     type: 'Uint8',
        //     size: 1,
        //     min: 0,
        //     max: 255,
        // },
        // {
        //     type: 'Uint16',
        //     size: 2,
        //     min: 0,
        //     max: 65535,
        // },
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
                offset: 0,
            },
            'Destination_airport': {
                type: 'string',
                max: 0,
                offset: 0,
            },
            'Origin_city': {
                type: 'string',
                max: 0,
                offset: 0,
            },
            'Destination_city': {
                type: 'string',
                max: 0,
                offset: 0,
            },
            'Passengers': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
                offset: 0,
            },
            'Seats': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
                offset: 0,
            },
            'Flights': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
                offset: 0,
            },
            'Distance': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
                offset: 0,
            },
            'Fly_date': {
                type: 'string',
                max: 0,
                offset: 0,
            },
            'Origin_population': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
                offset: 0,
            },
            'Destination_population': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
                offset: 0,
            },
            'Org_airport_lat': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
                offset: 0,
            },
            'Org_airport_long': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
                offset: 0,
            },
            'Dest_airport_lat': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
                offset: 0,
            },
            'Dest_airport_long': {
                type: 'number',
                isFloat: false,
                min: Number.MAX_SAFE_INTEGER,
                max: Number.MIN_SAFE_INTEGER,
                offset: 0,
            },
        },
        countRaw: 0,
        count: 0,
        rowSize: 0,
    };

    let start;
    let end;

    const keysOriginal = Object.keys(meta.columns);
    const keys = keysOriginal.slice().sort((a, b) => {
        if (a === 'Origin_airport' || a === 'Destination_airport') {
            if (b === 'Origin_airport' || b === 'Destination_airport') {
                return 0;
            }
            return 1;
        }

        if (b === 'Origin_airport' || b === 'Destination_airport') {
            if (a === 'Origin_airport' || a === 'Destination_airport') {
                return 0;
            }
            return -1;
        }

        if (meta.columns[a].type === meta.columns[b].type) {
            return 0;
        }

        if (meta.columns[a].type === 'string') {
            return 1;
        }

        return -1;
    });

    start = new Date();
    await processCSV(path.resolve(__dirname, '../files/Airports2.csv'), null, data => {
        if (!data) {
            return;
        }

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
                meta.columns[key].max = Math.max(meta.columns[key].max, data[key].length);
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
        columnOrderOriginal: keysOriginal,
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

        finalMeta.columns[key].offset = finalMeta.rowSize;

        finalMeta.rowSize += finalMeta.columns[key].size;
    });

    finalMeta.rowSize = 4 * Math.floor(finalMeta.rowSize / 4) + 4 * Math.min(finalMeta.rowSize % 4, 1);

    const headerString = JSON.stringify(finalMeta);
    const headerLength = 4 * Math.floor(headerString.length / 4) + 4 * Math.min(headerString.length % 4, 1);
    const header = new Uint8Array(headerLength);
    for (let i = 0; i < headerString.length; ++i) {
        header[i] = headerString.charCodeAt(i);
    }

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
    await processCSV(path.resolve(__dirname, '../files/Airports2.csv'), null, data => {
        if (!data) {
            return;
        }

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
                for (let i = 0, n = data[key].length; i < n; ++i) {
                    rowBuffer.writeUInt8(data[key].charCodeAt(i), rowOffset + i);
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
    const compressed = pako.deflate(result, { level: 9 });
    end = new Date();
    console.log(end - start);

    let output = path.resolve(__dirname, './metadata.json');
    fs.writeFile(output, JSON.stringify(finalMeta), err => {
        if (err) {
            return console.log(err);
        }
        console.log('Created ' + output);

        output = path.resolve(__dirname, './data.bin');
        fs.writeFile(output, compressed, err => {
            if (err) {
                return console.log(err);
            }
            console.log('Created ' + output);
        });
    });
}

main();
