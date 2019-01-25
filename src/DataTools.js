import {ByteString} from './ByteString';

export class DataTools {
    static generateColumnGetters(header, newStringInstances = false) {
        const getters = [];
        const keyMap = {};
        let rowOffset = 0;
        for (let i = 0, n = header.columnOrder.length; i < n; ++i) {
            const key = header.columnOrder[i];
            const column = header.columns[key];
            const off = rowOffset;

            keyMap[key] = getters.length;

            if (column.type === 'string' || column.type === 'date') {
                if (newStringInstances) {
                    getters.push(function getStringInstance(view, offset, target) {
                        target[key] = ByteString.fromDataView(view, offset + off, offset + off + column.size);
                    });
                } else {
                    const arr = new Uint8Array(column.size);
                    const byteString = new ByteString(arr);
                    getters.push(function getString(view, offset, target) {
                        byteString.setDataView(view, offset + off, offset + off + column.size);
                        target[key] = byteString;
                    });
                }
            } else {
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
