export class DataTools {
    static generateColumnGetters(header) {
        const getters = [];
        const keyMap = {};
        let rowOffset = 0;
        for (let i = 0, n = header.columnOrder.length; i < n; ++i) {
            const key = header.columnOrder[i];
            const column = header.columns[key];
            const off = rowOffset;
            if (column.type === 'string' || column.type === 'date') {
                const decoder = new TextDecoder();
                const arr = new Uint8Array(column.size);
                let ii;
                let nn;
                keyMap[key] = getters.length;
                getters.push(function getString(view, offset, target) {
                    for (ii = 0, nn = column.size; ii < nn; ++ii) {
                        arr[ii] = view.getUint8(offset + off + ii);
                    }
                    // target[key] = decoder.decode(arr);
                    target[key] = String.fromCharCode.apply(null, arr);
                });
            } else {
                keyMap[key] = getters.length;
                switch (column.type) {
                    case 'Int8':
                        getters.push(function getInt8(view, offset, target) {
                            target[key] = view.getInt8(offset + off);
                        });
                        break;

                    case 'Int16':
                        getters.push(function getInt16(view, offset, target) {
                            target[key] = view.getInt16(offset + off, true);
                        });
                        break;

                    case 'Int32':
                        getters.push(function getInt32(view, offset, target) {
                            target[key] = view.getInt32(offset + off, true);
                        });
                        break;

                    case 'Uint8':
                        getters.push(function getUint8(view, offset, target) {
                            target[key] = view.getUint8(offset + off);
                        });
                        break;

                    case 'Uint16':
                        getters.push(function getUint16(view, offset, target) {
                            target[key] = view.getUint16(offset + off, true);
                        });
                        break;

                    case 'Uint32':
                        getters.push(function getUint32(view, offset, target) {
                            target[key] = view.getUint32(offset + off, true);
                        });
                        break;

                    case 'Float32':
                        getters.push(function getFloat32(view, offset, target) {
                            target[key] = view.getFloat32(offset + off, true);
                        });
                        break;

                    default:
                        break;
                }
            }
            rowOffset += column.size;
        }

        return {
            getters,
            keyMap,
        };
    }

    static generateRowGetter(getters) {
        const n = getters.length;
        let i;
        return function getRow(view, offset, target) {
            for (i = 0; i < n; ++i) {
                getters[i](view, offset, target);
            }
        };
    }
}
