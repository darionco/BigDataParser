import {DataManager} from './DataManager';

function generateColumnGetters(header) {
    const getters = [];
    const keyMap = {};
    let rowOffset = 0;
    header.columnOrder.forEach(key => {
        const column = header.columns[key];
        const off = rowOffset;
        if (column.type === 'string' || column.type === 'date') {
            const decoder = new TextDecoder();
            const arr = new Uint8Array(column.size);
            let i;
            let n;
            keyMap[key] = getters.length;
            getters.push((view, offset, target) => {
                for (i = 0, n = column.size; i < n; ++i) {
                    arr[i] = view.getUint8(offset + off + i);
                }
                target[key] = decoder.decode(arr);
            });
        } else {
            keyMap[key] = getters.length;
            switch (column.type) {
                case 'Int8':
                    getters.push((view, offset, target) => {
                        target[key] = view.getInt8(offset + off);
                    });
                    break;

                case 'Int16':
                    getters.push((view, offset, target) => {
                        target[key] = view.getInt16(offset + off, true);
                    });
                    break;

                case 'Int32':
                    getters.push((view, offset, target) => {
                        target[key] = view.getInt32(offset + off, true);
                    });
                    break;

                case 'Uint8':
                    getters.push((view, offset, target) => {
                        target[key] = view.getUint8(offset + off);
                    });
                    break;

                case 'Uint16':
                    getters.push((view, offset, target) => {
                        target[key] = view.getUint16(offset + off, true);
                    });
                    break;

                case 'Uint32':
                    getters.push((view, offset, target) => {
                        target[key] = view.getUint32(offset + off, true);
                    });
                    break;

                case 'Float32':
                    getters.push((view, offset, target) => {
                        target[key] = view.getFloat32(offset + off, true);
                    });
                    break;

                default:
                    break;
            }
        }
        rowOffset += column.size;
    });

    return {
        getters,
        keyMap,
    };
}

function generateRowGetter(getters) {
    const n = getters.length;
    let i;
    return (view, offset, target) => {
        for (i = 0; i < n; ++i) {
            getters[i](view, offset, target);
        }
    };
}

function test(header, buffer, start) {
    const view = new DataView(buffer, start);
    const columnReader = generateColumnGetters(header);
    const rowReader = generateRowGetter(columnReader.getters);

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
    const encoder = new TextEncoder();

    const row = {};

    let i;
    let ii;
    let key;

    const startTime = new Date();
    for (i = 0; i < header.count; ++i) {
        rowReader(view, i * header.rowSize, row);

        let add = true;
        for (ii = 0; ii < header.columnOrder.length; ++ii) {
            key = header.columnOrder[ii];
            if (meta.columns[key].type === 'number') {
                if (isNaN(row[key])) {
                    add = false;
                } else {
                    const v = parseFloat(row[key]);
                    meta.columns[key].isFloat = meta.columns[key].isFloat || (v % 1 !== 0);
                    meta.columns[key].min = Math.min(meta.columns[key].min, v);
                    meta.columns[key].max = Math.max(meta.columns[key].max, v);
                }
            } else {
                // const s = encoder.encode(row[key]);
                meta.columns[key].max = Math.max(meta.columns[key].max, row[key].length);
            }
        }

        if (add) {
            meta.count += 1;
        }
        meta.countRaw += 1;
    }
    const endTime = new Date();
    console.log(endTime - startTime);

    console.log(meta);
    console.log(row);

    document.getElementById('totalTime').innerText = endTime - startTime;
}

function main() {
    let start;
    let end;

    const fileInput = document.createElement('input');
    fileInput.setAttribute('type', 'file');
    fileInput.setAttribute('name', 'dataFile');
    document.body.appendChild(fileInput);
    fileInput.addEventListener('change', e => {
        e.preventDefault();
        document.body.innerHTML = 'Loading...';

        const dataManager = new DataManager();
        start = new Date();
        dataManager.init(fileInput.files[0]).then(() => {
            end = new Date();

            let base = '';

            base += `<div>Loaded in: ${end - start}ms</div>`;
            base += `<div>Columns: ${dataManager.header.columnOrder.length}</div>`;
            base += `<div>Rows: ${dataManager.header.count.toLocaleString()}</div>`;

            let filter = '';
            filter += '<select id="selectColumn">';
            dataManager.header.columnOrder.forEach(column => {
                filter += `<option value="${column}">${column}</option>`;
            });
            filter += '</select>';

            filter += '<select id="selectOperation">';
            filter += `<option value="contains">contains</option>`;
            filter += `<option value="equal">equal</option>`;
            filter += `<option value="notEqual">not equal</option>`;
            filter += `<option value="moreThan">more than</option>`;
            filter += `<option value="lessThan">less than</option>`;
            filter += '</select>';

            filter += '<input type="text" id="filterValue">';
            filter += '<button id="runFilter"> search</button>';

            document.body.innerHTML = base + filter;

            document.addEventListener('click', click => {
                if (click.target && click.target.id === 'runFilter') {
                    click.preventDefault();

                    const selectColumn = document.getElementById('selectColumn').value;
                    const selectOperation = document.getElementById('selectOperation').value;
                    let filterValue = document.getElementById('filterValue').value;

                    if (filterValue.length) {
                        const columnType = dataManager.header.columns[selectColumn].type;

                        let isValid = false;
                        if (columnType === 'string' || columnType === 'date') {
                            isValid = selectOperation !== 'moreThan' && selectOperation !== 'lessThan';
                        } else if (!isNaN(filterValue)) {
                            filterValue = parseFloat(filterValue);
                            isValid = selectOperation !== 'contains';
                        }

                        if (isValid) {
                            const threads = 3;
                            let search = '';
                            search += `<div>Running filter with ${threads} threads...</div>`;
                            document.body.innerHTML = base + search;
                            start = new Date();
                            dataManager.test({
                                column: selectColumn,
                                operation: selectOperation,
                                value: filterValue,
                            }, threads).then(rows => {
                                end = new Date();
                                search += `<div>Completed in: ${end - start}ms</div>`;
                                search += `<div>Rows processed: ${dataManager.mIndicesView[0]}</div>`;
                                search += `<div>Results found: ${dataManager.mIndicesView[2]}</div>`;

                                let table = '<table>';
                                table += '<thead><tr>';
                                dataManager.header.columnOrder.forEach(column => {
                                    table += `<th>${column}</th>`;
                                });
                                table += '</tr></thead>';

                                table += '<tbody>';
                                rows.forEach(row => {
                                    table += '<tr>';
                                    dataManager.header.columnOrder.forEach(column => {
                                        table += `<td>${row[column]}</td>`;
                                    });
                                    table += '</tr>';
                                });
                                table += '</tbody>';

                                table += '</table>';

                                document.body.innerHTML = base + search + filter + table;
                            });
                        }
                    }
                }
            });
        });
    });
    //     e.preventDefault();
    //     const file = fileInput.files[0];
    //
    //     const reader = new FileReader();
    //     reader.addEventListener("loadend", () => {
    //         end = new Date();
    //         console.log(end - start);
    //         console.log(reader.result);
    //
    //         start = new Date();
    //         const view = new DataView(reader.result);
    //         end = new Date();
    //         console.log(end - start);
    //         console.log(view);
    //
    //         let offset = 0;
    //
    //         const headerSize = view.getUint32(offset, true);
    //         offset += 4;
    //         console.log(headerSize);
    //
    //         const headerView = new Uint8Array(reader.result, offset, headerSize);
    //         const decoder = new TextDecoder();
    //         const header = JSON.parse(decoder.decode(headerView));
    //         console.log(header);
    //
    //         offset += headerSize;
    //
    //         test(header, reader.result, offset);
    //     });
    //     start = new Date();
    //     reader.readAsArrayBuffer(file);
    //
    // });
}

document.addEventListener('DOMContentLoaded', () => main());
