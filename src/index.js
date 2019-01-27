import {DataManager} from './DataManager';

function main() {
    let start;
    let end;

    const fileInput = document.createElement('input');
    fileInput.setAttribute('type', 'file');
    fileInput.setAttribute('name', 'dataFile');
    document.body.appendChild(fileInput);
    fileInput.addEventListener('change', e => {
        e.preventDefault();
        document.body.innerHTML = 'Loading...';

        const dataManager = new DataManager();
        start = new Date();
        dataManager.init(fileInput.files[0]).then(() => {
            end = new Date();

            let threads = 1;
            let chunkSize = 1;
            let selectColumn = dataManager.header.columnOrder[0];
            let selectOperation = 'contains';
            let aggregationType = 'none';
            let filterValue = '';
            let threadsString = () => {
                let thread = '';
                thread += '<div>';
                thread += '<span>Thread count:</span>';
                thread += `<input type="number" id="threadCount" min="1" max="8" value="${threads}">`;
                thread += '<span> Chunk size:</span>';
                thread += `<input type="number" id="chunkSize" min="1" max="1000000" value="${chunkSize}">`;
                thread += '<span> Aggregation:</span>';
                thread += '<select id="aggregationType">';
                thread += `<option value="none" ${aggregationType === 'none' ? 'selected' : ''}>none</option>`;
                thread += `<option value="byRoute" ${aggregationType === 'byRoute' ? 'selected' : ''}>by route</option>`;
                thread += `<option value="WebGL" ${aggregationType === 'WebGL' ? 'selected' : ''}>WebGL Graph</option>`;
                thread += '</select>';
                thread += '</div>';
                return thread;
            };
            let base = '';

            base += `<div>Loaded in: ${end - start}ms</div>`;
            base += `<div>Columns: ${dataManager.header.columnOrder.length}</div>`;
            base += `<div>Rows: ${dataManager.header.count.toLocaleString()}</div>`;

            const filterString = () => {
                let filter = '';
                filter += '<span>Column:</span>';
                filter += '<select id="selectColumn">';
                dataManager.header.columnOrder.forEach(column => {
                    filter += `<option value="${column}" ${selectColumn === column ? 'selected' : ''}>${column}</option>`;
                });
                filter += '</select>';

                filter += '<span> Operator:</span>';
                filter += '<select id="selectOperation">';
                filter += `<option value="contains" ${selectOperation === 'contains' ? 'selected' : ''}>contains</option>`;
                filter += `<option value="equal" ${selectOperation === 'equal' ? 'selected' : ''}>equal</option>`;
                filter += `<option value="notEqual" ${selectOperation === 'notEqual' ? 'selected' : ''}>not equal</option>`;
                filter += `<option value="moreThan" ${selectOperation === 'moreThan' ? 'selected' : ''}>more than</option>`;
                filter += `<option value="lessThan" ${selectOperation === 'lessThan' ? 'selected' : ''}>less than</option>`;
                filter += '</select>';

                filter += '<span> Value:</span>';
                filter += `<input type="text" id="filterValue" value="${filterValue}">`;
                filter += '<button id="runFilter"> search</button>';

                return filter;
            };

            document.body.innerHTML = base + threadsString() + filterString();

            document.addEventListener('change', change => {
                if (change.target && change.target.id === 'threadCount') {
                    change.target.value = Math.max(Math.min(change.target.value, 8), 1);
                }
            });

            document.addEventListener('click', click => {
                if (click.target && click.target.id === 'runFilter') {
                    click.preventDefault();

                    selectColumn = document.getElementById('selectColumn').value;
                    selectOperation = document.getElementById('selectOperation').value;
                    filterValue = document.getElementById('filterValue').value;
                    threads = document.getElementById('threadCount').value;
                    chunkSize = document.getElementById('chunkSize').value;
                    aggregationType = document.getElementById('aggregationType').value;

                    if (filterValue.length) {
                        const columnType = dataManager.header.columns[selectColumn].type;

                        let isValid = false;
                        if (columnType === 'string' || columnType === 'date') {
                            isValid = selectOperation !== 'moreThan' && selectOperation !== 'lessThan';
                        } else if (!isNaN(filterValue)) {
                            filterValue = parseFloat(filterValue);
                            isValid = selectOperation !== 'contains';
                        }

                        if (isValid) {
                            let search = '';
                            search += `<div>Running filter with ${threads} threads...</div>`;
                            document.body.innerHTML = base + search;
                            start = new Date();
                            dataManager.test({
                                column: selectColumn,
                                operation: selectOperation,
                                value: filterValue,
                            }, chunkSize, threads, aggregationType).then(result => {
                                end = new Date();
                                search += `<div>Completed in: ${end - start}ms</div>`;
                                search += `<div>Rows processed: ${dataManager.mIndicesView[0]}</div>`;
                                search += `<div>Results found: ${dataManager.mIndicesView[2]}</div>`;

                                document.body.innerHTML = base + search;

                                setTimeout(() => {
                                    let table = '<table>';
                                    table += '<thead><tr>';
                                    dataManager.header.columnOrder.forEach(column => {
                                        table += `<th>${column}</th>`;
                                    });
                                    table += '</tr></thead>';

                                    table += '<tbody>';
                                    for (let i = 0; i < 200 && i < result.count; ++i) {
                                        const row = result.getRow(i);
                                        table += '<tr>';
                                        dataManager.header.columnOrder.forEach(column => { // eslint-disable-line
                                            table += `<td>${row[column]}</td>`;
                                        });
                                        table += '</tr>';
                                    }
                                    table += '</tbody>';

                                    table += '</table>';

                                    document.body.innerHTML = base + search + threadsString() + filterString() + table;
                                }, 100);
                            });
                        }
                    }
                }
            });
        });
    });
}

document.addEventListener('DOMContentLoaded', () => main());
