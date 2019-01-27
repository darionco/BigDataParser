const kCodeA = ('A').charCodeAt(0);
const kCodeZ = ('Z').charCodeAt(0);

const c1 = 0xcc9e2d51;
const c2 = 0xcc9e2d51;

function _murmurHash(key, seed = 0) {
    const remainder = key.mLength & 3;
    const bytes = key.mLength - remainder;
    let i = 0;
    let h1;
    let h1b;
    let k1;
    h1 = seed;

    while (i < bytes) {
        k1 =
            ((key.byteAt(i) & 0xff)) |
            ((key.byteAt(++i) & 0xff) << 8) |
            ((key.byteAt(++i) & 0xff) << 16) |
            ((key.byteAt(++i) & 0xff) << 24);
        ++i;

        k1 = ((((k1 & 0xffff) * c1) + ((((k1 >>> 16) * c1) & 0xffff) << 16))) & 0xffffffff;
        k1 = (k1 << 15) | (k1 >>> 17);
        k1 = ((((k1 & 0xffff) * c2) + ((((k1 >>> 16) * c2) & 0xffff) << 16))) & 0xffffffff;

        h1 ^= k1;
        h1 = (h1 << 13) | (h1 >>> 19);
        h1b = ((((h1 & 0xffff) * 5) + ((((h1 >>> 16) * 5) & 0xffff) << 16))) & 0xffffffff;
        h1 = (((h1b & 0xffff) + 0x6b64) + ((((h1b >>> 16) + 0xe654) & 0xffff) << 16));
    }

    k1 = 0;

    switch (remainder) { /* eslint-disable-line */
        case 3: k1 ^= (key.byteAt(i + 2) & 0xff) << 16; /* fallthrough */
        case 2: k1 ^= (key.byteAt(i + 1) & 0xff) << 8; /* fallthrough */
        case 1: k1 ^= (key.byteAt(i) & 0xff); /* fallthrough */

            k1 = (((k1 & 0xffff) * c1) + ((((k1 >>> 16) * c1) & 0xffff) << 16)) & 0xffffffff;
            k1 = (k1 << 15) | (k1 >>> 17);
            k1 = (((k1 & 0xffff) * c2) + ((((k1 >>> 16) * c2) & 0xffff) << 16)) & 0xffffffff;
            h1 ^= k1;
    }

    h1 ^= key.mLength;

    h1 ^= h1 >>> 16;
    h1 = (((h1 & 0xffff) * 0x85ebca6b) + ((((h1 >>> 16) * 0x85ebca6b) & 0xffff) << 16)) & 0xffffffff;
    h1 ^= h1 >>> 13;
    h1 = ((((h1 & 0xffff) * 0xc2b2ae35) + ((((h1 >>> 16) * 0xc2b2ae35) & 0xffff) << 16))) & 0xffffffff;
    h1 ^= h1 >>> 16;

    return h1 >>> 0;
}

export class ByteString {
    static fromString(value) {
        const arr = new Uint8Array(value.length);
        for (let i = 0, n = value.length; i < n; ++i) {
            arr[i] = value.charCodeAt(i);
        }
        return new ByteString(arr);
    }

    static fromTypedArray(value) {
        const arr = new Uint8Array(value.buffer);
        return new ByteString(arr, value.byteOffset, value.byteOffset + value.byteLength);
    }

    static fromArrayBuffer(value, start = 0, end = value.byteLength) {
        const arr = new Uint8Array(value);
        return new ByteString(arr, start, end);
    }

    static fromDataView(value, start = 0, end = value.byteLength) {
        const arr = new Uint8Array(value.buffer);
        return new ByteString(arr, value.byteOffset + start, value.byteOffset + end);
    }

    constructor(uint8, start = 0, end = uint8.length) {
        this.mData = uint8;
        this.mOffset = start;
        this.mSize = end - start;
        this.mLength = this.mSize;

        this._i = 0;
        this._n = 0;
        this._ii = 0;
        this._nn = 0;

        // for (this.mLength = 0; this.mLength < this.mSize; ++this.mLength) {
        //     if (this.mData[this.mOffset + this.mLength] === 0) {
        //         break;
        //     }
        // }
    }

    get length() {
        return this.mLength;
    }

    get hash() {
        return _murmurHash(this, 0xABCD);
    }

    setString(value) {
        this.mData = new Uint8Array(value.length);
        this.setStringUnsafe(value);
    }

    setStringUnsafe(value) {
        for (this.mLength = 0, this._n = value.length; this.mLength < this._n; ++this.mLength) {
            this.mData[this.mLength] = value.charCodeAt(this.mLength);
        }
    }

    setDataView(value, start = 0, end = value.byteLength) {
        if (value.buffer !== this.mData.buffer) {
            this.mData = new Uint8Array(value.buffer);
        }

        this.mOffset = value.byteOffset + start;
        this.mSize = end - start;
        this.mLength = this.mSize;

        // for (this.mLength = 0; this.mLength < this.mSize; ++this.mLength) {
        //     if (this.mData[this.mOffset + this.mLength] === 0) {
        //         break;
        //     }
        // }
    }

    toString() {
        return String.fromCharCode.apply(null, new Uint8Array(this.mData.buffer, this.mOffset, this.mLength)).trim();
    }

    byteAt(i) {
        return this.mData[this.mOffset + i];
    }

    equals(other) {
        if (other.mLength !== this.mLength) {
            return false;
        }

        for (this._i = 0; this._i < this.mLength; ++this._i) {
            if (other.byteAt(this._i) !== this.byteAt(this._i)) {
                return false;
            }
        }

        return true;
    }

    equalsCase(other) {
        if (other.mLength !== this.mLength) {
            return false;
        }

        for (this._i = 0; this._i < this.mLength; ++this._i) {
            if (this._toLower(other.byteAt(this._i)) !== this._toLower(this.byteAt(this._i))) {
                return false;
            }
        }

        return true;
    }

    contains(other) {
        this._nn = other.length;
        for (this._i = 0, this._n = this.mLength - this._nn; this._i < this._n; ++this._i) {
            if (this.byteAt(this._i) === other.byteAt(0)) {
                for (this._ii = 1; this._ii < this._nn; ++this._ii) {
                    if (this.byteAt(this._i + this._ii) !== other.byteAt(this._ii)) {
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
            if (this._toLower(this.byteAt(this._i)) === this._toLower(other.byteAt(0))) {
                for (this._ii = 1; this._ii < this._nn; ++this._ii) {
                    if (this._toLower(this.byteAt(this._i + this._ii)) !== this._toLower(other.byteAt(this._ii))) {
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

    copy(other) {
        for (this._i = 0; this._i < this.mSize; ++this._i) {
            this.mData[this.mOffset + this._i] = other.byteAt(this._i);
        }
    }

    _toLower(c) {
        return (c >= kCodeA && c <= kCodeZ) ? (c + 32) : c;
    }
}
