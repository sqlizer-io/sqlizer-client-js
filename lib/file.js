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
  async convert(timeout) {
    const startedAt = new Date().getTime();
    const createResult = await axios({
      method: "post",
      url: "https://sqlizer.io/api/files",
      headers: {
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

    if ([200, 201].includes(createResult.status)) {
      this.updateFromApi(createResult.data);

      await this.uploadFileContent();

      await this.waitForConversionToComplete(startedAt, timeout);
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
  async uploadFileContent() {
    const file = await openAsync(this.Path, "r");
    let PartNumber = 0;

    while (true) {
      const content = await this.readBlockFromFile(file);
      PartNumber += 1;

      if (content) {
        const form = new FormData();
        form.append("file", content, { filename: this.FileName });

        const uploadResult = await axios.post(
          `https://sqlizer.io/api/files/${this.ID}/data?PartNumber=${PartNumber}`,
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

    // Tell SQLizer that we're done uploading
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
  }

  /**
   * Reads the next block of the file described by `this.Path`
   *
   * @async
   * @param {*} file
   * @returns {unknown}
   */
  async readBlockFromFile(file) {
    const BLOCK_SIZE = 10000000,
      buf = Buffer.alloc(BLOCK_SIZE);

    const { buffer, bytesRead } = await readAsync(
      file,
      buf,
      0,
      BLOCK_SIZE,
      null
    );

    if (bytesRead === 0) {
      return;
    } else if (bytesRead < BLOCK_SIZE) {
      return buffer.slice(0, bytesRead);
    } else {
      return buffer;
    }
  }

  async waitForConversionToComplete(startedAt, timeout) {
    const gradualBackoff = 1.01;
    let interval = 500;

    while (["Queued", "Analysing", "Processing"].includes(this.Status)) {
      await sleep(interval);
      interval += gradualBackoff;

      if (timeout) {
        const now = new Date().getTime();
        if (now > startedAt + timeout) {
          throw new Error("Conversion timeout reached");
        }
      }

      const getResult = await axios({
        method: "get",
        url: `https://sqlizer.io/api/files/${this.ID}`,
        headers: {
          ...this.getAuthHeader(),
        },
      });

      this.updateFromApi(getResult.data);
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
   * Sets the status to "Failed" and captures any message value in the response
   *
   * @param {*} responseData
   */
  captureError(responseData) {
    this.Status = "Failed";
    ({ Message: this.Message } = responseData);
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

module.exports = SQLizerFile;
