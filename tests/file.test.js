const { SQLizerFile } = require("../lib/");
const nock = require("nock");

const defaultParams = {
  FileType: "csv",
  FileName: "test1.csv",
  TableName: "test1",
  DatabaseType: "MySQL",
  FileHasHeaders: true,
  Delimiter: ",",
  CheckTableExists: true,
  InsertSpacing: 250,
  Path: "./tests/test1.csv",
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

  describe("convert", () => {
    const fileId = "12345";

    nock("https://sqlizer.io")
      .post("/api/files")
      .reply(200, {
        ID: fileId,
        Status: "New",
      })
      .post(`/api/files/${fileId}/data`)
      .query(query => {
        return !!query.PartNumber;
      })
      .reply(200, {})
      .get(`/downloads/${fileId}`)
      .reply(200, "CREATE TABLE my_table...")
      .persist();

    it("should save the fields from the API", async () => {
      const responseObject = {
        ID: "12345",
        Status: "Complete",
        ResultUrl: `https://sqlizer.io/downloads/${fileId}`,
      };

      nock("https://sqlizer.io")
        .put(`/api/files/${fileId}`)
        .reply(200, responseObject);

      const file = new SQLizerFile(defaultParams);
      await file.convert();
      expect(file.ID).toBe(fileId);
      expect(file.Status).toBe(responseObject.Status);
    });

    it("should keep calling the API until the Status reaches Complete", async () => {
      nock("https://sqlizer.io").put(`/api/files/${fileId}`).reply(200, {
        ID: "12345",
        Status: "Queued",
      });

      const scope = nock("https://sqlizer.io")
        .get(`/api/files/${fileId}`)
        .reply(200, {
          ID: "12345",
          Status: "Analysing",
        })
        .get(`/api/files/${fileId}`)
        .reply(200, {
          ID: "12345",
          Status: "Processing",
        })
        .get(`/api/files/${fileId}`)
        .reply(200, {
          ID: "12345",
          Status: "Complete",
          ResultUrl: `https://sqlizer.io/downloads/${fileId}`,
        });

      const file = new SQLizerFile(defaultParams);
      await file.convert();
      expect(file.ID).toBe(fileId);
      expect(file.Status).toBe("Complete");
      expect(scope.isDone()).toBe(true);
    });

    it("should time out if the timeout is reached", async () => {
      nock("https://sqlizer.io").put(`/api/files/${fileId}`).reply(200, {
        ID: "12345",
        Status: "Queued",
      });

      const scope = nock("https://sqlizer.io")
        .get(`/api/files/${fileId}`)
        .times(10)
        .reply(200, {
          ID: "12345",
          Status: "Analysing",
        });

      const file = new SQLizerFile(defaultParams);
      let error;
      try {
        await file.convert(3000); // 3 second timeout
      } catch (err) {
        error = err;
      }
      expect(error).toBeTruthy();
      expect(error.message).toBe("Conversion timeout reached");
      expect(file.ID).toBe(fileId);
      expect(file.Status).toBe("Analysing");
    });

    it("should capture any error message from the initial upload", async () => {
      nock("https://sqlizer.io").put(`/api/files/${fileId}`).reply(200, {
        Status: "Failed",
        Message: "That file is too big",
      });

      const file = new SQLizerFile(defaultParams);
      let error;
      try {
        await file.convert();
      } catch (err) {
        error = err;
      }
      expect(error).toBeTruthy();
      expect(file.Status).toBe("Failed");
      expect(file.Message).toBe("That file is too big");
    });

    it("should capture any error message from during the conversion", async () => {
      nock("https://sqlizer.io").put(`/api/files/${fileId}`).reply(200, {
        ID: "12345",
        Status: "Queued",
      });

      const scope = nock("https://sqlizer.io")
        .get(`/api/files/${fileId}`)
        .reply(200, {
          Status: "Failed",
          Message: "That file is incorrectly formatted",
        });

      const file = new SQLizerFile(defaultParams);
      let error;
      try {
        await file.convert();
      } catch (err) {
        error = err;
      }
      expect(error).toBeTruthy();
      expect(file.Status).toBe("Failed");
      expect(file.Message).toBe("That file is incorrectly formatted");
    });
  });
});
