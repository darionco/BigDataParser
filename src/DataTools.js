/* global Atomics */

import {ByteString} from './dataStructures/ByteString';

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
                    getters.push(function getStringInstance(view, offset) {
                        return ByteString.fromDataView(view, offset + off, offset + off + column.size);
                    });
                } else {
                    const arr = new Uint8Array(column.size);
                    const byteString = new ByteString(arr);
                    getters.push(function getString(view, offset) {
                        byteString.setDataView(view, offset + off, offset + off + column.size);
                        return byteString;
                    });
                }
            } else {
                switch (column.type) {
                    case 'Int8':
                        getters.push(function getInt8(view, offset) {
                            return view.getInt8(offset + off);
                        });
                        break;

                    case 'Int16':
                        getters.push(function getInt16(view, offset) {
                            return view.getInt16(offset + off, true);
                        });
                        break;

                    case 'Int32':
                        getters.push(function getInt32(view, offset) {
                            return view.getInt32(offset + off, true);
                        });
                        break;

                    case 'Uint8':
                        getters.push(function getUint8(view, offset) {
                            return view.getUint8(offset + off);
                        });
                        break;

                    case 'Uint16':
                        getters.push(function getUint16(view, offset) {
                            return view.getUint16(offset + off, true);
                        });
                        break;

                    case 'Uint32':
                        getters.push(function getUint32(view, offset) {
                            return view.getUint32(offset + off, true);
                        });
                        break;

                    case 'Float32':
                        getters.push(function getFloat32(view, offset) {
                            return view.getFloat32(offset + off, true);
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
            order: header.columnOrder,
        };
    }

    static generateRowGetter(columnGetters) {
        const getters = columnGetters.getters;
        const order = columnGetters.order;
        const n = getters.length;
        let i;
        return function getRow(view, offset, target) {
            for (i = 0; i < n; ++i) {
                target[order[i]] = getters[i](view, offset);
            }
        };
    }

    static generateAggregationFunction(aggregation) {
        switch (aggregation) {
            case 'byRoute':
            case 'WebGL':
            case 'none': {
                let resultIndex;
                let i;
                return (result, indicesView, dataView, rowIndex, rowSize) => {
                    resultIndex = Atomics.add(indicesView, 2, 1);
                    if (resultIndex < indicesView[3]) {
                        for (i = 0; i < rowSize; ++i) {
                            result.mDataView.setUint8(resultIndex * rowSize + i, dataView.getUint8(rowIndex + i));
                        }
                    }
                };
            }

            default:
                return () => null;
        }
    }
}
