# sqlizer-client-js

A JavaScript client library for SQLizer.io, easily converting CSV, JSON, XML and Spreadsheet files into SQL

## Getting Started

Install the library:

```bash
npm i sqlizer-client
```

Then import the `File` class and use it to convert a file:

```JavaScript
const { createWriteStream } = require('fs');
const { File } = require('../lib');

const sqlizerFile = new File({
  ApiKey: '[MY API KEY]',
  FileType: 'csv',
  FileName: 'my-file.csv',
  TableName: 'my_table',
  DatabaseType: 'SQLite',
  FileHasHeaders: true,
  Delimiter: ',',
  CheckTableExists: true,
  InsertSpacing: 150,
  Path: './my-file.csv'
});

// Create a writable stream to store the generated SQL
var writeStream = createWriteStream('./my-result.sql', { flags : 'w' });

// Ask SQLizer to run the conversion and pipe the results to our file
sqlizerFile.convert().then(results => results.pipe(writeStream));
```
