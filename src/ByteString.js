const kCodeA = ('A').charCodeAt(0);
const kCodeZ = ('Z').charCodeAt(0);

export class ByteString {
    static fromString(value) {
        const arr = new Uint8Array(value.length);
        for (let i = 0, n = value.length; i < n; ++i) {
            arr[i] = value.charCodeAt(i);
        }
        return new ByteString(arr);
    }

    static fromTypedArray(value) {
        const arr = new Uint8Array(value.buffer.byteLength);
        const view = new Uint8Array(value.buffer);
        arr.set(view);
        return new ByteString(arr);
    }

    static fromArrayBuffer(value, start = 0, end = value.byteLength) {
        const arr = new Uint8Array(end - start);
        const view = new Uint8Array(value, start, end - start);
        arr.set(view);
        return new ByteString(arr);
    }

    static fromDataView(value, start = 0, end = value.byteLength) {
        const arr = new Uint8Array(end - start);
        for (let i = start; i < end; ++i) {
            arr[i - start] = value.getUint8(i);
        }
        return new ByteString(arr);
    }

    constructor(typedArray) {
        this.mData = typedArray;
        this.mLength = 0;

        this._i = 0;
        this._n = 0;
        this._ii = 0;
        this._nn = 0;

        for (this.mLength = 0, this._n = this.mData.length; this.mLength < this._n; ++this.mLength) {
            if (this.mData[this.mLength] === 0) {
                break;
            }
        }
    }

    get length() {
        return this.mLength;
    }

    setString(value) {
        if (this.mData.length < value.length) {
            this.mData = new Uint8Array(value.length);
        }
        this.setStringUnsafe(value);
    }

    setStringUnsafe(value) {
        for (this.mLength = 0, this._n = value.length; this.mLength < this._n; ++this.mLength) {
            this.mData[this.mLength] = value.charCodeAt(this.mLength);
        }
    }

    setDataView(value, start = 0, end = value.byteLength) {
        if (this.mData.length < end - start) {
            this.mData = new Uint8Array(end - start);
        }
        this.setDataViewUnsafe(value, start, end);
    }

    setDataViewUnsafe(value, start, end) {
        for (this.mLength = 0, this._n = end - start; this.mLength < this._n; ++this.mLength) {
            this.mData[this.mLength] = value.getUint8(this.mLength + start);
            if (this.mData[this.mLength] === 0) {
                return;
            }
        }
    }

    toString() {
        return String.fromCharCode.apply(null, this.mData);
    }

    toLowercase() {
        for (this._i = 0; this._i < this.mLength; ++this._i) {
            this.mData[this._i] = this._toLower(this.mData[this._i]);
        }
        return this;
    }

    equals(other) {
        if (other.mLength !== this.mLength) {
            return false;
        }

        for (let i = 0; i < this.mLength; ++i) {
            if (other.mData[i] !== this.mData[i]) {
                return false;
            }
        }

        return true;
    }

    equalsCase(other) {
        if (other.mLength !== this.mLength) {
            return false;
        }

        for (let i = 0; i < this.mLength; ++i) {
            if (this._toLower(other.mData[i]) !== this._toLower(this.mData[i])) {
                return false;
            }
        }

        return true;
    }

    contains(other) {
        this._nn = other.length;
        for (this._i = 0, this._n = this.mLength - this._nn; this._i < this._n; ++this._i) {
            if (this.mData[this._i] === other.mData[0]) {
                for (this._ii = 1; this._ii < this._nn; ++this._ii) {
                    if (this.mData[this._i + this._ii] !== other.mData[this._ii]) {
                        break;
                    }
                }
                if (this._ii === this._nn) {
                    return true;
                }
            }
        }
        return false;
    }

    containsCase(other) {
        this._nn = other.length;
        for (this._i = 0, this._n = 1 + this.mLength - this._nn; this._i < this._n; ++this._i) {
            if (this._toLower(this.mData[this._i]) === this._toLower(other.mData[0])) {
                for (this._ii = 1; this._ii < this._nn; ++this._ii) {
                    if (this._toLower(this.mData[this._i + this._ii]) !== this._toLower(other.mData[this._ii])) {
                        break;
                    }
                }
                if (this._ii === this._nn) {
                    return true;
                }
            }
        }
        return false;
    }

    _toLower(c) {
        return (c >= kCodeA && c <= kCodeZ) ? (c + 32) : c;
    }
}
