/* global Atomics, SharedArrayBuffer */

import DataWorker from 'worker-loader!./Data.worker';
import pako from 'pako';
import {DataTools} from './DataTools';
import {HashTable} from './dataStructures/HashTable';
import pakoUtils from 'pako/lib/utils/common';

/* patch flattenChunks to use SharredArrayBuffer for memory backing */
pakoUtils.flattenChunks = chunks => {
    let i;
    let l;
    let len;
    let pos;
    let chunk;
    let result;
    let backing;

    // calculate data length
    len = 0;
    for (i = 0, l = chunks.length; i < l; i++) {
        len += chunks[i].length;
    }

    // join chunks
    backing = new SharedArrayBuffer(len);
    result = new Uint8Array(backing);
    pos = 0;
    for (i = 0, l = chunks.length; i < l; i++) {
        chunk = chunks[i];
        result.set(chunk, pos);
        pos += chunk.length;
    }

    return result;
};

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

        let nb;
        for (nb = 0; nb < headerSize; ++nb) {
            if (view.getUint8(3 + headerSize - nb) !== 0) {
                break;
            }
        }

        const headerView = new Uint8Array(this.mSharedMemory, 4, headerSize - nb);

        this.mHeader = JSON.parse(String.fromCharCode.apply(null, headerView));
        this.mDataOffset = headerSize + 4;

        this.mColumnGetters = DataTools.generateColumnGetters(this.mHeader);
        this.mRowReader = DataTools.generateRowGetter(this.mColumnGetters);

        await this._loadWorkers();
    }

    test(filter, chunkSize, threadCount = this.mWorkers.length, aggregation = 'none') {
        const maxResults = 35000;

        Atomics.store(this.mIndicesView, 0, 0);
        Atomics.store(this.mIndicesView, 1, this.mHeader.count);
        Atomics.store(this.mIndicesView, 2, 0);
        Atomics.store(this.mIndicesView, 3, maxResults);

        const result = new HashTable(maxResults, 6, this.mHeader.rowSize);

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
                    result: result.serialize(),
                    chunk: chunkSize,
                    aggregation,
                });
            }));
        }
        return Promise.all(promises).then(() => {
            const view = new DataView(result.mData);
            const count = Math.min(this.mIndicesView[2], this.mIndicesView[3]);
            const row = {};
            return {
                count,
                getRow: index => {
                    this.mRowReader(view, index * this.mHeader.rowSize, row);
                    return row;
                },
            };
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
        const compressed = await this._readBytesFromChunk(file);
        const result = pako.inflate(compressed);
        return result.buffer;
        // const memory = new ArrayBuffer(size);
        // const view = new Uint8Array(memory);
        //
        //
        // let offset = 0;
        // let chunk;
        // let data;
        // while(offset < size) {
        //     chunk = file.slice(offset, Math.min(offset + chunkSize, size));
        //     data = await this._readBytesFromChunk(chunk);
        //     view.set(data, offset);
        //     offset += chunkSize;
        // }
        // return memory;
    }

    _readBytesFromChunk(chunk) {
        return new Promise(resolve => {
            const reader = new FileReader();
            reader.addEventListener('loadend', () => {
                resolve(new Uint8Array(reader.result));
            });
            reader.readAsArrayBuffer(chunk);
        });
    }
}
