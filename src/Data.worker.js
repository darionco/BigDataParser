/* global Atomics */

import {DataTools} from './DataTools';
import {ByteString} from './dataStructures/ByteString';

class DataProcessor {
    constructor(options) {
        this.mID = options.id;
        this.mSharedMemory = options.memory;
        this.mDataOffset = options.offset;
        this.mSharedIndices = options.indices;
        this.mHeader = options.header;

        this.mDataView = new DataView(this.mSharedMemory, this.mDataOffset);
        this.mIndicesView = new Uint32Array(this.mSharedIndices);

        this.mColumnGetters = DataTools.generateColumnGetters(this.mHeader);
        this.mRowReader = DataTools.generateRowGetter(this.mColumnGetters);
    }

    test(chunkSize, filter, result) {
        const columnGetters = this.mColumnGetters;
        const testFilter = this._getFilterFunction(filter);
        const testColumn = columnGetters.keyMap[filter.column];
        const dataView = this.mDataView;
        const indicesView = this.mIndicesView;
        const resultView = new DataView(result);
        const rowSize = this.mHeader.rowSize;
        // const row = {};
        let resultIndex;
        let rowIndex;
        let i;
        let ii;
        let iii;

        for (i = Atomics.add(indicesView, 0, chunkSize);
            i < indicesView[1]/* && Atomics.load(indicesView, 2) < indicesView[3]*/;
            i = Atomics.add(indicesView, 0, chunkSize)) {
            for (ii = 0; ii < chunkSize && i + ii < indicesView[1]; ++ii) {
                rowIndex = (i + ii) * rowSize;
                if (testFilter(columnGetters.getters[testColumn](dataView, rowIndex))) {
                    resultIndex = Atomics.add(indicesView, 2, 1);
                    if (resultIndex < indicesView[3]) {
                        for (iii = 0; iii < rowSize; ++iii) {
                            resultView.setUint8(resultIndex * rowSize + iii, dataView.getUint8(rowIndex + iii));
                        }
                    }
                    // this.mRowReader(dataView, (i + ii) * rowSize, row);
                    // this._aggregate(row, dataView, (i + ii) * rowSize, container, indicesView[3], info);
                }
            }
        }
        global.postMessage('success');
    }

    _aggregationNone() {

    }

    _aggregationByRoute() {

    }

    _aggregationWebGL() {

    }

    _getFilterFunction(filter) {
        switch (filter.operation) {
            case 'contains': {
                const value = ByteString.fromString(filter.value);
                return function filterContains(toTest) { return toTest.containsCase(value); };
            }

            case 'equal':
                if (this.mHeader.columns[filter.column].type === 'string' || this.mHeader.columns[filter.column].type === 'date') {
                    const value = ByteString.fromString(filter.value);
                    return function filterEquals(toTest) { return toTest.equalsCase(value); };
                }
                return function filterEquals(toTest) { return toTest === filter.value; };

            case 'notEqual':
                if (this.mHeader.columns[filter.column].type === 'string' || this.mHeader.columns[filter.column].type === 'date') {
                    const value = ByteString.fromString(filter.value);
                    return function filterEquals(toTest) { return !toTest.equalsCase(value); };
                }
                return function filterNotEqual(toTest) { return toTest !== filter.value; };

            case 'moreThan':
                return function filterMoreThan(toTest) { return toTest > filter.value; };

            case 'lessThan':
                return function filterLessThan(toTest) { return toTest < filter.value; };

            default:
                break;
        }
        return null;
    }
}

let processor = null;

global.onmessage = function dataWorkerOnMessage(e) {
    const message = e.data;
    if (message.type === 'init') {
        processor = new DataProcessor(message);
        global.postMessage('success');
    } else if (message.type === 'test') {
        processor.test(message.chunk, message.filter, message.result);
    }
};
