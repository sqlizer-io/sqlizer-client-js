const fs = require("fs");
const axios = require("axios").default;
const { promisify } = require("util");
const FormData = require("form-data");
const validateTypes = require("validate-types");

const readAsync = promisify(fs.read);
const openAsync = promisify(fs.open);

const optionsSchema = require("./options-schema");

const sleep = async function (timeout) {
  return new Promise((resolve) => setTimeout(resolve, timeout));
};

/**
 * Represents a file to be converted into SQL by SQLizer.
 *
 * @class SQLizerFile
 * @typedef {SQLizerFile}
 */
class SQLizerFile {
  /**
   * Creates an instance of File.
   *
   * @constructor
   * @param {*} options
   */
  constructor(options) {
    var validationResult = validateTypes(optionsSchema, options);
    if (validationResult.hasErrors) {
      throw new Error("Options parameter validation failed", {
        cause: validationResult.errors,
      });
    }
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
      Path: this.Path,
    } = options);

    this.ID = undefined;
    this.Message = undefined;
    this.PercentComplete = undefined;
    this.ResultRows = undefined;
    this.ResultUrl = undefined;
  }

  /**
   * Calls the SQLizer API to convert the file, returning a promise that
   * resolves to a stream containing the results.
   *
   * @async
   * @returns {stream.Readable}
   */
  async convert() {
    const createResult = await axios({
      method: "post",
      url: "https://sqlizer.io/api/files/",
      header: {
        "Content-Type": "application/json",
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
        Status: "New",
      },
    });

    this.updateFromApi(createResult.data);

    await this.uploadFile();

    const updateResult = await axios({
      method: "put",
      url: `https://sqlizer.io/api/files/${this.ID}`,
      headers: {
        ...this.getAuthHeader(),
      },
      data: {
        Status: "Uploaded",
      },
    });

    this.updateFromApi(updateResult.data);

    while (["Queued", "Analysing", "Processing"].includes(this.Status)) {
      await sleep(500);

      const getResult = await axios({
        method: "get",
        url: `https://sqlizer.io/api/files/${this.ID}`,
        headers: {
          ...this.getAuthHeader(),
        },
      });

      this.updateFromApi(getResult.data);
    }

    if (this.Status === "Complete") {
      const downloadResult = await axios({
        method: "get",
        url: this.ResultUrl,
        responseType: "stream",
      });
      return downloadResult.data;
    } else if (this.Status === "SubscriptionRequired") {
      throw new Error("Subscription required to convert this file");
    } else if (this.Status === "PaymentRequired") {
      throw new Error("Payment required to convert this file");
    } /*if (this.Status === 'Failed')*/ else {
      throw new Error(this.Message || "Unknown error");
    }
  }

  /**
   * Uploads the contents of the file described by `this.Path`
   * in chunks, to the SQLizer /data endpoint.
   *
   * @async
   */
  async uploadFile() {
    const file = await openAsync(this.Path, "r");
    let PartNumber = 0;

    while (true) {
      const { content } = await this.readBlockFromFile(file);
      PartNumber += 1;

      if (content) {
        const form = new FormData();
        form.append("PartNumber", PartNumber);
        form.append("file", content, { filename: this.FileName });

        const uploadResult = await axios.post(
          `https://sqlizer.io/api/files/${this.ID}/data`,
          form,
          {
            headers: {
              ...this.getAuthHeader(),
              ...form.getHeaders(),
            },
          }
        );
      } else {
        break;
      }
    }
  }

  /**
   * Reads the next block of the file described by `this.Path`
   *
   * @async
   * @param {*} file
   * @returns {unknown}
   */
  async readBlockFromFile(file) {
    const BLOCK_SIZE = 10 * 1024 * 1024, // 10MB
      buf = Buffer.alloc(BLOCK_SIZE);

    const { buffer, bytesRead } = await readAsync(
      file,
      buf,
      0,
      BLOCK_SIZE,
      null
    );

    if (bytesRead === 0) {
      return { content: null };
    } else if (bytesRead < BLOCK_SIZE) {
      return { content: buffer.slice(0, bytesRead) };
    } else {
      return { content: buffer };
    }
  }

  /**
   * Updates the fields on `this` from the properties on the JSON response from the SQLizer API
   *
   * @param {*} responseData
   */
  updateFromApi(responseData) {
    ({
      ID: this.ID,
      Message: this.Message,
      PercentComplete: this.PercentComplete,
      ResultUrl: this.ResultUrl,
      ResultRows: this.ResultRows,
      Status: this.Status,
    } = responseData);
  }

  /**
   * Returns the Authorization header to use for the SQLizer API
   *
   * @returns {({ Authorization: string; } | { Authorization?: undefined; })}
   */
  getAuthHeader() {
    return this.ApiKey ? { Authorization: `Bearer ${this.ApiKey}` } : {};
  }
}

module.exports = {
  SQLizerFile,
};
