export class DataTools {
    static generateColumnGetters(header) {
        const getters = [];
        const keyMap = {};
        let rowOffset = 0;
        header.columnOrder.forEach(key => {
            const column = header.columns[key];
            const off = rowOffset;
            if (column.type === 'string' || column.type === 'date') {
                const decoder = new TextDecoder();
                const arr = new Uint8Array(column.size);
                let i;
                let n;
                keyMap[key] = getters.length;
                getters.push((view, offset, target) => {
                    for (i = 0, n = column.size; i < n; ++i) {
                        arr[i] = view.getUint8(offset + off + i);
                    }
                    target[key] = decoder.decode(arr);
                });
            } else {
                keyMap[key] = getters.length;
                switch (column.type) {
                    case 'Int8':
                        getters.push((view, offset, target) => {
                            target[key] = view.getInt8(offset + off);
                        });
                        break;

                    case 'Int16':
                        getters.push((view, offset, target) => {
                            target[key] = view.getInt16(offset + off, true);
                        });
                        break;

                    case 'Int32':
                        getters.push((view, offset, target) => {
                            target[key] = view.getInt32(offset + off, true);
                        });
                        break;

                    case 'Uint8':
                        getters.push((view, offset, target) => {
                            target[key] = view.getUint8(offset + off);
                        });
                        break;

                    case 'Uint16':
                        getters.push((view, offset, target) => {
                            target[key] = view.getUint16(offset + off, true);
                        });
                        break;

                    case 'Uint32':
                        getters.push((view, offset, target) => {
                            target[key] = view.getUint32(offset + off, true);
                        });
                        break;

                    case 'Float32':
                        getters.push((view, offset, target) => {
                            target[key] = view.getFloat32(offset + off, true);
                        });
                        break;

                    default:
                        break;
                }
            }
            rowOffset += column.size;
        });

        return {
            getters,
            keyMap,
        };
    }

    static generateRowGetter(getters) {
        const n = getters.length;
        let i;
        return (view, offset, target) => {
            for (i = 0; i < n; ++i) {
                getters[i](view, offset, target);
            }
        };
    }
}
