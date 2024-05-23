
const fs = require('fs');
const axios = require('axios').default;
const { promisify } = require('util');
const FormData = require('form-data');

const readAsync = promisify(fs.read);
const openAsync = promisify(fs.open);

class File {

  constructor(options) {
    ({
      ApiKey: this.ApiKey,
      FileType: this.FileType,
      FileName: this.FileName,
      TableName: this.TableName,
      DatabaseType: this.DatabaseType,
      FileHasHeaders: this.FileHasHeaders,
      Delimiter: this.Delimiter,
      CheckTableExists: this.CheckTableExists,
      InsertSpacing: this.InsertSpacing,
      Path: this.Path
    } = options);

    this.ID = undefined;
    this.Message = undefined;
    this.PercentComplete = undefined;
    this.ResultRows = undefined;
    this.ResultUrl = undefined;
  }

  async convert() {
    const createResult = await axios({
      method: 'post',
      url: 'https://sqlizer.io/api/files/',
      header: {
        'Content-Type': 'application/json',
        ...this.getAuthHeader(),
      },
      data: {
        FileType: this.FileType,
        FileName: this.FileName,
        TableName: this.TableName,
        DatabaseType: this.DatabaseType,
        FileHasHeaders: this.FileHasHeaders,
        Delimiter: this.Delimiter,
        CheckTableExists: this.CheckTableExists,
        InsertSpacing: this.InsertSpacing,
        Status: 'New'
      }
    });

    this.updateFromApi(createResult.data);

    await this.uploadFile();

    const updateResult = await axios({
      method: 'put',
      url: `https://sqlizer.io/api/files/${this.ID}`,
      headers: {
        ...this.getAuthHeader(),
      },
      data: {
        Status: 'Uploaded'
      }
    });

    this.updateFromApi(updateResult.data);

    while (['Queued', 'Analysing', 'Processing'].includes(this.Status)) {

      const getResult = await axios({
        method: 'get',
        url: `https://sqlizer.io/api/files/${this.ID}`,
        headers: {
          ...this.getAuthHeader(),
        },
      });

      this.updateFromApi(getResult.data);
    }

    if (this.Status === 'Complete') {
      const downloadResult = await axios({
        method: 'get',
        url: this.ResultUrl,
        responseType: 'stream'
      });
      return downloadResult.data;
    }
    else if (this.Status === 'SubscriptionRequired') {
      throw new Error('Subscription required to convert this file');
    }
    else if (this.Status === 'PaymentRequired') {
      throw new Error('Payment required to convert this file');
    }
    else /*if (this.Status === 'Failed')*/ {
      throw new Error(this.Message || 'Unknown error');
    }
  }

  async uploadFile() {
    const file = await openAsync(this.Path, 'r');
    let PartNumber = 0;

    while (true) {

      const { content } = await this.readBlockFromFile(file);
      PartNumber += 1;

      if (content) {          
        const form = new FormData();
        form.append('PartNumber', PartNumber);
        form.append('file', content, { filename : this.FileName });
      
        const uploadResult = await axios.post(`https://sqlizer.io/api/files/${this.ID}/data`, form, {
          headers: {
            ...this.getAuthHeader(),
            ...form.getHeaders(),
          }
        });
      }
      else {
        break;
      }
    }
  }

  async readBlockFromFile(file) {
    const BLOCK_SIZE = 10 * 1024 * 1024, // 10MB
      buf = Buffer.alloc(BLOCK_SIZE);

    const { buffer, bytesRead } = await readAsync(file, buf, 0, BLOCK_SIZE, null);

    if (bytesRead === 0) {
      return { content: null };
    }
    else if (bytesRead < BLOCK_SIZE) {
      return { content: buffer.slice(0, bytesRead) };
    }
    else {
      return { content: buffer }
    }
  }

  updateFromApi(responseData) {
    ({
      ID: this.ID,
      Message: this.Message, 
      PercentComplete: this.PercentComplete, 
      ResultUrl: this.ResultUrl, 
      ResultRows: this.ResultRows, 
      Status: this.Status 
    } = responseData);
  }

  getAuthHeader() {
    return this.ApiKey 
      ? { 'Authorization': `Bearer ${this.ApiKey}` }
      : {};
  }
}

module.exports = {
  File
}