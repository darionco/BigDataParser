/* global Atomics, SharedArrayBuffer */
import {ByteString} from './ByteString';

const kUint8Size = 1;
const kUint16Size = 2;
const kUint32Size = 4;

const kNilAddress = 0;
const kLockAddress = 1;
const kFullAddress = 2;
const kPaddingAddress = 3;

export class HashTable {
    static deserialize(description) {
        return new HashTable(description.size, description.keySize, description.rowSize, description.memoryBuffers);
    }

    constructor(size, keySize, rowSize, memoryBuffers = null) {
        this.mSize = size;
        this.mKeySize = keySize;
        this.mRowSize = rowSize;

        /**
         * Index Memory Layout
         *
         * Uint32: First node index + memory offset for this index
         */
        this.mIndexSize = kUint32Size;

        /**
         * Node Memory Layout
         *
         * Uint32: Key memory address
         * Uint32: Data memory address
         * Uint32: Next node index + memory offset or lock if being added
         */
        this.mNodeSize = 3 * kUint32Size;

        this.mIndexMemorySize = this.mSize * this.mIndexSize;
        this.mListsMemorySize = this.mSize * this.mNodeSize;
        this.mKeysMemorySize = this.mSize * this.mKeySize;

        this.mLengthOffset = kUint8Size * 4;
        this.mIndexOffset = this.mLengthOffset + kUint32Size;
        this.mListsOffset = this.mIndexOffset + this.mIndexMemorySize;
        this.mKeysOffset = this.mListsOffset + this.mListsMemorySize;

        if (memoryBuffers) {
            this.mState = memoryBuffers.state;
            this.mData = memoryBuffers.data;
        } else {
            /**
             * State Memory Layout
             *
             * Uint8: nil (end of list)
             * Uint8: lock (for multi-threading)
             * Uint8: full (flag to avoid computations when the table is full)
             * Uint8: padding (reserved)
             * Uint32: length (the length of the table)
             * this.mIndexMemorySize: index (hash to node index table)
             * this.mListsMemorySize: lists (collision solver lists)
             * this.mKeysMemorySize: keys (ByteString instances)
             */
            this.mState = new SharedArrayBuffer(
                (4 * kUint8Size) +
                (kUint32Size) +
                this.mIndexMemorySize +
                this.mListsMemorySize +
                this.mKeysMemorySize
            );

            /**
             * Data Memory Layout
             * this.mSize * this.mRowSize: data (rows)
             */
            this.mData = new SharedArrayBuffer(this.mSize * this.mRowSize);
        }

        this.mKeyString = ByteString.fromArrayBuffer(this.mState, this.mKeysOffset, this.mKeysOffset + this.mKeySize);

        this.mDataView = new DataView(this.mData);
        this.mStateView = new DataView(this.mState);

        this.mLengthView = new Uint32Array(this.mState, this.mLengthOffset, 1);
        this.mIndexView = new Uint32Array(this.mState, this.mIndexOffset, this.mIndexMemorySize / 4);
        this.mListsView = new Uint32Array(this.mState, this.mListsOffset, this.mListsMemorySize / 4);
    }

    serialize() {
        return {
            size: this.mSize,
            keySize: this.mKeySize,
            rowSize: this.mRowSize,
            memoryBuffers: {
                state: this.mState,
                data: this.mData,
            },
        };
    }

    addRecord(key, append, modify) {
        const index = key.hash % this.mSize;
        let listAddress = Atomics.compareExchange(this.mIndexView, index, kNilAddress, kLockAddress);

        if (listAddress === kNilAddress) {
            this._appendNewRecord(key, index, append);
        } else {
            if (listAddress === kLockAddress) {
                Atomics.wait(this.mIndexView, index, kLockAddress);
                listAddress = Atomics.load(this.mIndexView, index);
            }

            if (listAddress !== kFullAddress) {
                // modify
            }
        }
    }

    _appendNewRecord(key, index, append) {
        const entryIndex = Atomics.add(this.mLengthView, 0, 1);
        if (entryIndex >= this.mSize) {
            Atomics.sub(this.mLengthView, 0, 1);
            Atomics.store(this.mIndexView, index, kFullAddress);
            Atomics.notify(this.mIndexView, index);
            return;
        }

        /**
         * Node Memory Layout
         *
         * Uint32: Key memory address
         * Uint32: Data memory address
         * Uint32: Next node index + memory offset or lock if being added
         */
        const keyAddress = this.mKeysOffset + this.mKeySize * entryIndex;
        const dataAddress = this.mRowSize * entryIndex;

        this.mListsView[entryIndex] = keyAddress;
        this.mListsView[entryIndex + 1] = dataAddress;
        this.mListsView[entryIndex + 2] = 0;

        /* append the key */
        this.mKeyString.setDataView(this.mState, keyAddress, keyAddress + this.mKeySize);
        this.mKeyString.copy(key);

        /* call the user provided function */
        append(this.mDataView, dataAddress);

        /* store the index and notify other threads */
        Atomics.store(this.mIndexView, index, entryIndex + this.mListsOffset);
        Atomics.notify(this.mIndexView, index);
    }
}
