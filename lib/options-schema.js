module.exports = {
  ApiKey: String,
  FileType: {
    type: String,
    oneOf: ['csv', 'json', 'xlsx', 'xml'],
    required: true
  },
  FileName: {
    type: String,
    required: true
  },
  TableName: {
    type: String,
    required: true
  },
  DatabaseType: {
    type: String,
    oneOf: ['MySQL', 'SQLServer', 'PostgreSQL', 'SQLite'],
    required: true
  },
  FileHasHeaders: Boolean,
  Delimiter: {
    type: String,
    maxLength: 2
  },
  CheckTableExists: Boolean,
  InsertSpacing: {
    type: Number,
    integer: true
  },
  Path: {
    type: String,
    required: true
  }
}