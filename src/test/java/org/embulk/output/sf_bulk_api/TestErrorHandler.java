package org.embulk.output.sf_bulk_api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sforce.soap.partner.IError;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.StatusCode;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.soap.partner.fault.ExceptionCode;
import com.sforce.soap.partner.sobject.SObject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestErrorHandler {
  private static final Gson GSON =
      new GsonBuilder().disableHtmlEscaping().serializeNulls().create();

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private Schema schema;
  private Path errorFilePath;

  @Before
  public void setup() throws IOException {
    tempFolder.create();
    errorFilePath = tempFolder.getRoot().toPath().resolve("error_log.jsonl");

    // Create a simple schema for testing
    List<Column> columns = new ArrayList<>();
    columns.add(new Column(0, "id", Types.LONG));
    columns.add(new Column(1, "name", Types.STRING));
    columns.add(new Column(2, "email", Types.STRING));
    columns.add(new Column(3, "active", Types.BOOLEAN));
    columns.add(new Column(4, "score", Types.DOUBLE));
    schema = new Schema(columns);
  }

  @After
  public void tearDown() throws IOException {
    // Clean up temp files
    Files.deleteIfExists(errorFilePath);
  }

  @Test
  public void testErrorHandlerWithoutFileOutput() {
    // Test ErrorHandler without file output (original behavior)
    ErrorHandler handler = new ErrorHandler(schema);

    SObject sObject = createTestSObject("1", "Test User", "test@example.com", true, 95.5);
    ApiFault fault = createTestApiFault(ExceptionCode.INVALID_FIELD, "Name field is required");

    long errorCount = handler.handleFault(Arrays.asList(sObject), fault);
    assertEquals(1, errorCount);

    handler.close();
  }

  @Test
  public void testErrorHandlerWithFileOutput() throws IOException {
    // Test ErrorHandler with file output
    ErrorHandler handler = new ErrorHandler(schema, errorFilePath.toString(), 0);

    SObject sObject = createTestSObject("2", "John Doe", "john@example.com", false, 88.0);
    ApiFault fault = createTestApiFault(ExceptionCode.INVALID_FIELD, "Invalid field: custom__c");

    long errorCount = handler.handleFault(Arrays.asList(sObject), fault);
    assertEquals(1, errorCount);

    handler.close();

    // Verify error file was created with correct format
    Path taskFilePath = Paths.get(errorFilePath.toString() + "_task000.jsonl");
    assertTrue(Files.exists(taskFilePath));

    List<String> lines = Files.readAllLines(taskFilePath);
    assertEquals(1, lines.size());

    JsonObject jsonObject = new JsonParser().parse(lines.get(0)).getAsJsonObject();
    assertTrue(jsonObject.has("record_data"));
    assertTrue(jsonObject.has("error_code"));
    assertTrue(jsonObject.has("error_message"));

    JsonObject recordData = jsonObject.getAsJsonObject("record_data");
    assertEquals(2.0, recordData.get("id").getAsDouble(), 0.01);
    assertEquals("John Doe", recordData.get("name").getAsString());
    assertEquals("john@example.com", recordData.get("email").getAsString());
    assertEquals(false, recordData.get("active").getAsBoolean());
    assertEquals(88.0, recordData.get("score").getAsDouble(), 0.01);

    assertEquals("INVALID_FIELD", jsonObject.get("error_code").getAsString());
    assertEquals("Invalid field: custom__c", jsonObject.get("error_message").getAsString());
  }

  @Test
  public void testHandleErrorsWithSaveResults() throws IOException {
    ErrorHandler handler = new ErrorHandler(schema, errorFilePath.toString(), 1);

    List<SObject> sObjects = new ArrayList<>();
    sObjects.add(createTestSObject("3", "Alice", "alice@example.com", true, 75.5));
    sObjects.add(createTestSObject("4", "Bob", "bob@example.com", false, 60.0));

    SaveResult[] results = new SaveResult[2];
    results[0] = createSuccessSaveResult();
    results[1] =
        createFailedSaveResult(
            StatusCode.REQUIRED_FIELD_MISSING, "Email is required", new String[] {"Email"});

    long errorCount = handler.handleErrors(sObjects, results);
    assertEquals(1, errorCount);

    handler.close();

    // Verify only failed record is written to error file
    Path taskFilePath = Paths.get(errorFilePath.toString() + "_task001.jsonl");
    assertTrue(Files.exists(taskFilePath));

    List<String> lines = Files.readAllLines(taskFilePath);
    assertEquals(1, lines.size());

    JsonObject jsonObject = new JsonParser().parse(lines.get(0)).getAsJsonObject();
    JsonObject recordData = jsonObject.getAsJsonObject("record_data");
    assertEquals(4.0, recordData.get("id").getAsDouble(), 0.01);
    assertEquals("Bob", recordData.get("name").getAsString());
    assertEquals("REQUIRED_FIELD_MISSING", jsonObject.get("error_code").getAsString());
    assertTrue(jsonObject.get("error_message").getAsString().contains("Email is required"));
    assertTrue(jsonObject.get("error_message").getAsString().contains("[fields: Email]"));
  }

  @Test
  public void testHandleErrorsWithUpsertResults() throws IOException {
    ErrorHandler handler = new ErrorHandler(schema, errorFilePath.toString(), 2);

    List<SObject> sObjects = new ArrayList<>();
    sObjects.add(createTestSObject("5", "Charlie", "charlie@example.com", true, 92.0));
    sObjects.add(createTestSObject("6", "David", "david@example.com", false, 78.5));

    UpsertResult[] results = new UpsertResult[2];
    results[0] = createSuccessUpsertResult();
    results[1] =
        createFailedUpsertResult(
            StatusCode.DUPLICATE_VALUE, "Duplicate external ID", new String[] {"ExternalId__c"});

    long errorCount = handler.handleErrors(sObjects, results);
    assertEquals(1, errorCount);

    handler.close();

    Path taskFilePath = Paths.get(errorFilePath.toString() + "_task002.jsonl");
    assertTrue(Files.exists(taskFilePath));

    List<String> lines = Files.readAllLines(taskFilePath);
    assertEquals(1, lines.size());

    JsonObject jsonObject = new JsonParser().parse(lines.get(0)).getAsJsonObject();
    assertEquals("DUPLICATE_VALUE", jsonObject.get("error_code").getAsString());
  }

  @Test
  public void testMultipleErrors() throws IOException {
    ErrorHandler handler = new ErrorHandler(schema, errorFilePath.toString(), 3);

    List<SObject> sObjects = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      sObjects.add(
          createTestSObject(
              String.valueOf(i), "User" + i, "user" + i + "@example.com", i % 2 == 0, i * 10.0));
    }

    SaveResult[] results = new SaveResult[5];
    for (int i = 0; i < 5; i++) {
      if (i % 2 == 0) {
        results[i] = createSuccessSaveResult();
      } else {
        results[i] =
            createFailedSaveResult(
                StatusCode.FIELD_CUSTOM_VALIDATION_EXCEPTION,
                "Validation failed for User" + i,
                new String[] {"Field" + i});
      }
    }

    long errorCount = handler.handleErrors(sObjects, results);
    assertEquals(2, errorCount); // Errors for indices 1 and 3

    handler.close();

    Path taskFilePath = Paths.get(errorFilePath.toString() + "_task003.jsonl");
    assertTrue(Files.exists(taskFilePath));

    List<String> lines = Files.readAllLines(taskFilePath);
    assertEquals(2, lines.size());
  }

  @Test(expected = AbortException.class)
  public void testAbortOnInvalidSession() {
    ErrorHandler handler = new ErrorHandler(schema);

    SObject sObject = createTestSObject("7", "Test", "test@example.com", true, 50.0);
    ApiFault fault = createTestApiFault(ExceptionCode.INVALID_SESSION_ID, "Session expired");

    handler.handleFault(Arrays.asList(sObject), fault);
  }

  @Test
  public void testNullErrorMessage() throws IOException {
    ErrorHandler handler = new ErrorHandler(schema, errorFilePath.toString(), 4);

    SObject sObject = createTestSObject("8", "NullTest", "null@example.com", true, 100.0);
    ApiFault fault = createTestApiFault(ExceptionCode.INVALID_FIELD, null);

    long errorCount = handler.handleFault(Arrays.asList(sObject), fault);
    assertEquals(1, errorCount);

    handler.close();

    Path taskFilePath = Paths.get(errorFilePath.toString() + "_task004.jsonl");
    assertTrue(Files.exists(taskFilePath));

    List<String> lines = Files.readAllLines(taskFilePath);
    JsonObject jsonObject = new JsonParser().parse(lines.get(0)).getAsJsonObject();
    assertEquals("", jsonObject.get("error_message").getAsString());
  }

  @Test
  public void testEmptyOutputPath() {
    // Test that empty string output path doesn't create file
    ErrorHandler handler1 = new ErrorHandler(schema, "", 0);
    ErrorHandler handler2 = new ErrorHandler(schema, null, 0);

    SObject sObject = createTestSObject("9", "Test", "test@example.com", false, 77.7);
    ApiFault fault = createTestApiFault(ExceptionCode.INVALID_FIELD, "Invalid");

    handler1.handleFault(Arrays.asList(sObject), fault);
    handler2.handleFault(Arrays.asList(sObject), fault);

    handler1.close();
    handler2.close();

    // No file should be created
    Path taskFilePath1 = Paths.get("_task000.jsonl");
    Path taskFilePath2 = Paths.get("null_task000.jsonl");
    assertTrue(!Files.exists(taskFilePath1));
    assertTrue(!Files.exists(taskFilePath2));
  }

  private SObject createTestSObject(
      String id, String name, String email, boolean active, double score) {
    SObject obj = new SObject();
    obj.setType("TestObject");
    obj.setField("id", Long.parseLong(id));
    obj.setField("name", name);
    obj.setField("email", email);
    obj.setField("active", active);
    obj.setField("score", score);
    return obj;
  }

  private ApiFault createTestApiFault(ExceptionCode code, String message) {
    ApiFault fault = new ApiFault();
    fault.setExceptionCode(code);
    fault.setExceptionMessage(message);
    return fault;
  }

  private SaveResult createSuccessSaveResult() {
    SaveResult result = new SaveResult();
    result.setSuccess(true);
    result.setId("12345");
    return result;
  }

  private SaveResult createFailedSaveResult(StatusCode code, String message, String[] fields) {
    SaveResult result = new SaveResult();
    result.setSuccess(false);
    IError error = createError(code, message, fields);
    result.setErrors(new com.sforce.soap.partner.Error[] {convertToError(error)});
    return result;
  }

  private com.sforce.soap.partner.Error convertToError(IError iError) {
    com.sforce.soap.partner.Error error = new com.sforce.soap.partner.Error();
    error.setStatusCode(iError.getStatusCode());
    error.setMessage(iError.getMessage());
    error.setFields(iError.getFields());
    return error;
  }

  private UpsertResult createSuccessUpsertResult() {
    UpsertResult result = new UpsertResult();
    result.setSuccess(true);
    result.setCreated(true);
    result.setId("12345");
    return result;
  }

  private UpsertResult createFailedUpsertResult(StatusCode code, String message, String[] fields) {
    UpsertResult result = new UpsertResult();
    result.setSuccess(false);
    IError error = createError(code, message, fields);
    result.setErrors(new com.sforce.soap.partner.Error[] {convertToError(error)});
    return result;
  }

  private IError createError(StatusCode code, String message, String[] fields) {
    return new IError() {
      @Override
      public StatusCode getStatusCode() {
        return code;
      }

      @Override
      public void setStatusCode(StatusCode statusCode) {
        // Not used in tests
      }

      @Override
      public String getMessage() {
        return message;
      }

      @Override
      public void setMessage(String message) {
        // Not used in tests
      }

      @Override
      public String[] getFields() {
        return fields;
      }

      @Override
      public void setFields(String[] fields) {
        // Not used in tests
      }

      @Override
      public com.sforce.soap.partner.IExtendedErrorDetails[] getExtendedErrorDetails() {
        return null;
      }

      @Override
      public void setExtendedErrorDetails(com.sforce.soap.partner.IExtendedErrorDetails[] details) {
        // Not used in tests
      }
    };
  }
}
