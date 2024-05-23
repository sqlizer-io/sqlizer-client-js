const { createWriteStream } = require('fs');
const { File } = require('../lib');

const sqlizerFile = new File({
  FileType: 'csv',
  FileName: 'my-file.csv',
  TableName: 'my_table',
  DatabaseType: 'SQLite',
  FileHasHeaders: true,
  Delimiter: ',',
  CheckTableExists: true,
  InsertSpacing: 150,
  Path: './test1.csv'
});

// Create a writable stream to store the generated SQL
var writeStream = createWriteStream('./test1.sql', { flags : 'w' });

// Ask SQLizer to run the conversion and pipe the results to our file
sqlizerFile.convert().then(results => results.pipe(writeStream));