const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');
const ws = fs.createWriteStream("out.csv");

const folder = 'assets';
const file = 'trades_file.csv';

fs.createReadStream(path.resolve(__dirname, folder, file))
    .pipe(csv.parse({ headers: true }))
    .pipe(csv.format({ headers: true }))
    .transform((row, next) => {

        let date = new Date(row.createdAt);

        let quantities = ((row) => {
            let temp_quantities = {};
            if(row.side === "BUY") {
                temp_quantities["received"] = parseFloat(row.size);
                temp_quantities["sent"] = parseFloat(row.size) * parseFloat(row.price);
            } else {
                temp_quantities["received"] = parseFloat(row.size) * parseFloat(row.price);
                temp_quantities["sent"] = parseFloat(row.size);
            }
            return temp_quantities;
        })(row);

        let currencies = ((row) => {
            let temp_currencies = {};
            let pair = row.market.split("-");
            if(row.side === "BUY") {
                temp_currencies["received"] = pair[0];
                temp_currencies["sent"] = pair[1];
            } else {
                temp_currencies["received"] = pair[1];
                temp_currencies["sent"] = pair[0];
            }
            return temp_currencies;
        })(row);

        return next(null, {
            "Date": `${1+date.getUTCMonth()}/${date.getUTCDate()}/${date.getUTCFullYear()} ${date.getUTCHours()}:${date.getUTCMinutes()}:${date.getUTCSeconds()}`,
            "Received Quantity": quantities["received"].toFixed(8),
            "Received Currency": currencies["received"],
            "Sent Quantity": quantities["sent"].toFixed(8),
            "Sent Currency": currencies["sent"],
            "Fee Amount": parseFloat(row.fee).toFixed(8),
            "Fee Currency": "USD",
            "Tag": ""
        });
    })
    .pipe(ws)
    .on('end', () => process.exit());