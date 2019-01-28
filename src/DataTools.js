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

    static generateAggregationFunction(header, aggregation) {
        switch (aggregation) {
            case 'none': {
                let resultIndex;
                let i;
                return function aggregateNone(result, indicesView, dataView, rowOffset, rowSize) {
                    resultIndex = Atomics.add(indicesView, 2, 1);
                    if (resultIndex < indicesView[3]) {
                        for (i = 0; i < rowSize; ++i) {
                            result.mDataView.setUint8(resultIndex * rowSize + i, dataView.getUint8(rowOffset + i));
                        }
                    }
                };
            }

            case 'byRoute': {
                const keyString = new ByteString(new Uint8Array(4));
                const passengersOffset = header.columns.Passengers.offset;
                const seatsOffset = header.columns.Seats.offset;
                const flightsOffset = header.columns.Flights.offset;
                const passengersIntOffset = passengersOffset / 4;
                const seatsIntOffset = seatsOffset / 4;
                const flightsIntOffset = flightsOffset / 4;
                let intOffset;
                let i;
                return function aggregateByRoute(result, indicesView, dataView, rowOffset, rowSize) {
                    keyString.setDataView(dataView, rowOffset + rowSize - 6, rowOffset + rowSize);
                    result.addRecord(
                        keyString,
                        function appendByRoute(view, offset) {
                            Atomics.add(indicesView, 2, 1);
                            view.setUint32(offset, 1, true);
                            for (i = 0; i < rowSize; ++i) {
                                view.setUint8(4 + offset + i, dataView.getUint8(rowOffset + i));
                            }
                        },
                        function modifyByRoute(numeric, offset) {
                            intOffset = offset / 4;
                            Atomics.add(numeric, intOffset, 1);
                            Atomics.add(numeric, 1 + intOffset + passengersIntOffset, dataView.getUint32(rowOffset + passengersOffset, true));
                            Atomics.add(numeric, 1 + intOffset + seatsIntOffset, dataView.getUint32(rowOffset + seatsOffset, true));
                            Atomics.add(numeric, 1 + intOffset + flightsIntOffset, dataView.getUint32(rowOffset + flightsOffset, true));
                        },
                    );
                };
            }

            case 'WebGL': {
                const keyString = new ByteString(new Uint8Array(4));

                const orgLngOffset = header.columns.Org_airport_long.offset;
                const orgLatOffset = header.columns.Org_airport_lat.offset;
                const dstLngOffset = header.columns.Dest_airport_long.offset;
                const dstLatOffset = header.columns.Dest_airport_lat.offset;
                const passenOffset = header.columns.Passengers.offset;
                const distOffset = header.columns.Distance.offset;

                let weight;
                let length;

                return function aggregateWebGL(result, indicesView, dataView, rowOffset, rowSize) {
                    keyString.setDataView(dataView, rowOffset + rowSize - 6, rowOffset + rowSize);
                    result.addRecord(
                        keyString,
                        function appendByRoute(view, offset) {
                            Atomics.add(indicesView, 2, 1);

                            view.setFloat32(offset, dataView.getFloat32(rowOffset + orgLngOffset, true), true);
                            view.setFloat32(offset + 4, dataView.getFloat32(rowOffset + orgLatOffset, true), true);
                            view.setFloat32(offset + 8, dataView.getFloat32(rowOffset + dstLngOffset, true), true);
                            view.setFloat32(offset + 12, dataView.getFloat32(rowOffset + dstLatOffset, true), true);

                            weight = dataView.getUint32(rowOffset + passenOffset, true);
                            length = dataView.getUint32(rowOffset + distOffset, true);

                            view.setUint32(offset + 16, weight, true);
                            view.setUint32(offset + 20, length, true);

                            result.__edgeValues.minWeight = Math.min(result.__edgeValues.minWeight, weight);
                            result.__edgeValues.maxWeight = Math.max(result.__edgeValues.maxWeight, weight);
                            result.__edgeValues.minLength = Math.min(result.__edgeValues.minLength, length);
                            result.__edgeValues.maxLength = Math.max(result.__edgeValues.maxLength, length);
                        },
                        function modifyByRoute(numeric, offset) {
                            weight = dataView.getUint32(rowOffset + passenOffset, true);

                            weight += Atomics.add(numeric, (offset / 4) + 4, weight);

                            result.__edgeValues.minWeight = Math.min(result.__edgeValues.minWeight, weight);
                            result.__edgeValues.maxWeight = Math.max(result.__edgeValues.maxWeight, weight);
                        }
                    );
                };
            }

            default:
                return () => null;
        }
    }
}
