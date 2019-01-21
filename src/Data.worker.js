/* global Atomics */

import {DataTools} from './DataTools';

const kIndexMap = {
    loopIndex: 0,
    loopTarget: 1,
    resultCount: 2,
    resultTarget: 3,
};

class DataProcessor {
    constructor(options) {
        this.mID = options.id;
        this.mSharedMemory = options.memory;
        this.mDataOffset = options.offset;
        this.mSharedIndices = options.indices;
        this.mHeader = options.header;

        this.mMemoryView = new DataView(this.mSharedMemory, this.mDataOffset);
        this.mIndicesView = new Uint32Array(this.mSharedIndices);

        this.mColumnGetters = DataTools.generateColumnGetters(this.mHeader);
        this.mRowReader = DataTools.generateRowGetter(this.mColumnGetters.getters);
    }

    test(chunkSize, filter, result) {
        const testFilter = this._getFilterFunction(filter);
        const row = {};
        const resultView = new Uint8Array(result);
        for (let i = Atomics.add(this.mIndicesView, 0, chunkSize);
             i < this.mIndicesView[1] && Atomics.load(this.mIndicesView, 2) < this.mIndicesView[3];
             i = Atomics.add(this.mIndicesView, 0, chunkSize)) {
            for (let ii = 0; ii < chunkSize && i + ii < this.mIndicesView[1]; ++ii) {
                this.mRowReader(this.mMemoryView, (i + ii) * this.mHeader.rowSize, row);
                if (testFilter(row)) {
                    const resultIndex = Atomics.add(this.mIndicesView, 2, 1);
                    if (resultIndex < this.mIndicesView[3]) {
                        for (let iii = 0; iii < this.mHeader.rowSize; ++iii) {
                            resultView[resultIndex * this.mHeader.rowSize + iii] = this.mMemoryView.getUint8((i + ii) * this.mHeader.rowSize + iii);
                        }
                    }
                }
            }
        }
        global.postMessage('success');
    }

    _getFilterFunction(filter) {
        switch (filter.operation) {
            case 'contains': {
                const value = filter.value.toLowerCase();
                return function filterContains(row) { return row[filter.column].toLowerCase().indexOf(value) !== -1; };
            }

            case 'equal':
                return function filterEquals(row) { return row[filter.column] === filter.value; };

            case 'notEqual':
                return function filterNotEqual(row) { return row[filter.column] !== filter.value; };

            case 'moreThan':
                return function filterMoreThan(row) { return row[filter.column] > filter.value; };

            case 'lessThan':
                return function filterLessThan(row) { return row[filter.column] < filter.value; };

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
