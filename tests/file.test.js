const { SQLizerFile } = require("../lib/file.js");

const defaultParams = {
  FileType: "csv",
  FileName: "test1.csv",
  TableName: "test1",
  DatabaseType: "MySQL",
  FileHasHeaders: true,
  Delimiter: ",",
  CheckTableExists: true,
  InsertSpacing: 250,
  Path: "./test1.csv",
};

describe("SQLizerFile", () => {
  describe("constructor", () => {
    function testRequiredParameter(parameterName) {
      const params = {
        ...defaultParams,
      };
      delete params[parameterName];
      let error = undefined;
      try {
        new SQLizerFile(params);
      } catch (err) {
        error = err;
      }
      expect(error).toBeInstanceOf(Error);
      expect(error.cause).toContainEqual({
        field: parameterName,
        test: "required",
        message: `Field "${parameterName}" is required`,
      });
    }

    it("should throw if FileType is not passed", () =>
      testRequiredParameter("FileType"));
    it("should throw if FileName is not passed", () =>
      testRequiredParameter("FileName"));
    it("should throw if TableName is not passed", () =>
      testRequiredParameter("TableName"));
    it("should throw if DatabaseType is not passed", () =>
      testRequiredParameter("DatabaseType"));
    it("should throw if Path is not passed", () =>
      testRequiredParameter("Path"));
  });
});
