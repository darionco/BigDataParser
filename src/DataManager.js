/* global Atomics, SharedArrayBuffer */

import DataWorker from 'worker-loader!./Data.worker';
import {DataTools} from './DataTools';

export class DataManager {
    constructor() {
        this.mSharedIndices = new SharedArrayBuffer(16);
        this.mIndicesView = new Uint32Array(this.mSharedIndices);
        this.mWorkers = [];
        this.mSharedMemory = null;
        this.mMemoryView = null;
        this.mHeader = null;
        this.mDataOffset = 0;
    }

    get header() {
        return this.mHeader;
    }

    async init(file) {
        this.mSharedMemory = await this._loadFileIntoSharedMemory(file);

        const view = new DataView(this.mSharedMemory);
        const headerSize = view.getUint32(0, true);
        const headerView = new Uint8Array(headerSize);
        const decoder = new TextDecoder();
        headerView.set(new Uint8Array(this.mSharedMemory, 4, headerSize));
        this.mHeader = JSON.parse(decoder.decode(headerView));
        this.mDataOffset = headerSize + 4;

        this.mColumnGetters = DataTools.generateColumnGetters(this.mHeader, true);
        this.mRowReader = DataTools.generateRowGetter(this.mColumnGetters.getters);

        await this._loadWorkers();
    }

    test(filter, chunkSize, threadCount = this.mWorkers.length) {
        const maxResults = 1000;

        Atomics.store(this.mIndicesView, 0, 0);
        Atomics.store(this.mIndicesView, 1, this.mHeader.count);
        Atomics.store(this.mIndicesView, 2, 0);
        Atomics.store(this.mIndicesView, 3, maxResults);

        const result = new SharedArrayBuffer(maxResults * this.mHeader.rowSize);

        const promises = [];
        for (let i = 0, n = Math.min(threadCount, this.mWorkers.length); i < n; ++i) {
            promises.push(new Promise(resolve => {
                const worker = this.mWorkers[i];
                worker.onmessage = e => {
                    const message = e.data;
                    if (message === 'success') {
                        worker.onmessage = event => this._handleWorkerMessage(event.data);
                        resolve();
                    } else {
                        throw 'Worker failed to initialize!';
                    }
                };

                worker.postMessage({
                    type: 'test',
                    filter,
                    result: result,
                    chunk: chunkSize,
                });
            }));
        }
        return Promise.all(promises).then(() => {
            const ret = [];
            const view = new DataView(result);
            const count = Math.min(this.mIndicesView[2], this.mIndicesView[3]);
            for (let i = 0; i < count; ++i) {
                const row = {};
                this.mRowReader(view, i * this.mHeader.rowSize, row);
                ret.push(row);
            }
            return ret;
        });
    }

    _loadWorkers() {
        const promises = [];
        if (!this.mWorkers.length) {
            for (let i = 0; i < 8; ++i) {
                promises.push(new Promise(resolve => {
                    const worker = new DataWorker();
                    worker.onmessage = e => {
                        const message = e.data;
                        if (message === 'success') {
                            worker.onmessage = event => this._handleWorkerMessage(event.data);
                            resolve();
                        } else {
                            throw 'Worker failed to initialize!';
                        }
                    };
                    worker.postMessage({
                        type: 'init',
                        id: i,
                        memory: this.mSharedMemory,
                        indices: this.mSharedIndices,
                        header: this.mHeader,
                        offset: this.mDataOffset,
                    });
                    this.mWorkers.push(worker);
                }));
            }
        }
        return Promise.all(promises);
    }

    async _loadFileIntoSharedMemory(file) {
        const size = file.size;
        const memory = new SharedArrayBuffer(size);
        const view = new Uint8Array(memory);
        const chunkSize = 100000000;

        let offset = 0;
        let chunk;
        let data;
        while(offset < size) {
            chunk = file.slice(offset, Math.min(offset + chunkSize, size));
            data = await this._readBytesFromChunk(chunk);
            view.set(data, offset);
            offset += chunkSize;
        }
        return memory;
    }

    _handleWorkerMessage(message) {

    }

    _readBytesFromChunk(chunk) {
        return new Promise(resolve => {
            const reader = new FileReader();
            reader.addEventListener("loadend", () => {
                resolve(new Uint8Array(reader.result));
            });
            reader.readAsArrayBuffer(chunk);
        });
    }
}
