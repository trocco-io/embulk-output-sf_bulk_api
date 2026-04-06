package org.embulk.output.sf_bulk_api;

import static org.embulk.output.sf_bulk_api.Util.mockActionResponse;
import static org.embulk.output.sf_bulk_api.Util.mockResponse;
import static org.embulk.output.sf_bulk_api.Util.newDefaultConfigSource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.Column;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.type.Types;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestSfBulkApiFileOutputPlugin {
  protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  private MockWebServer mockWebServer;

  @Before
  public void setup() throws IOException {
    mockWebServer = new MockWebServer();
    mockWebServer.start(Util.SERVER_PORT);
  }

  @After
  public void tearDown() throws IOException {
    mockWebServer.shutdown();
  }

  @Rule
  public TestingEmbulk embulk =
      TestingEmbulk.builder()
          .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
          .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
          .registerPlugin(OutputPlugin.class, "sf_bulk_api", SfBulkApiOutputPlugin.class)
          .build();

  // ========== Existing basic tests ==========

  @Test
  public void testInsert() throws IOException, InterruptedException {
    testSuccessRun("insert");
  }

  @Test
  public void testUpdate() throws IOException, InterruptedException {
    testSuccessRun("update");
  }

  @Test
  public void testUpsert() throws IOException, InterruptedException {
    testSuccessRun("upsert");
  }

  @Test
  public void testIgnoreNullsTrue() throws IOException, InterruptedException {
    testSuccessRun(
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("ignore_nulls", true),
        new Column[] {
          new Column(0, "id", Types.STRING), new Column(1, "test", Types.STRING),
        },
        new String[] {"id0,"},
        new String[] {"id", "_index"},
        new String[] {"string", "double"},
        new String[] {"id0,1.0"});
  }

  @Test
  public void testIgnoreNullsFalse() throws IOException, InterruptedException {
    testSuccessRun(
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("ignore_nulls", false),
        new Column[] {
          new Column(0, "id", Types.STRING), new Column(1, "test", Types.STRING),
        },
        new String[] {"id0,"},
        new String[] {"id", "_index", "fieldsToNull"},
        new String[] {"string", "double", "string"},
        new String[] {"id0,1.0,test"});
  }

  @Test
  public void testThrowIfFailedTrue() throws IOException, InterruptedException {
    testThrowIfFailed(true);
  }

  @Test
  public void testThrowIfFailedFalse() throws IOException, InterruptedException {
    testThrowIfFailed(false);
  }

  public void testThrowIfFailed(Boolean throwIfFailed) throws IOException, InterruptedException {
    ConfigSource config =
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("throw_if_failed", throwIfFailed);
    PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);

    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(mockActionResponse(task.getActionType(), new Boolean[] {false}));
    File in = Util.createInputFile(testFolder, "id:string", "id0");
    if (throwIfFailed) {
      assertThrows(PartialExecutionException.class, () -> embulk.runOutput(config, in.toPath()));
    } else {
      embulk.runOutput(config, in.toPath());
    }
    // login + action only; no logout call
    assertEquals(2, mockWebServer.getRequestCount());
    assertEquals(
        Util.readResource("loginRequestBody.xml"),
        Util.toStringFromGZip(mockWebServer.takeRequest()));
    String expectedBody =
        Util.actionRequestBody(
            task.getActionType(),
            new String[] {"id", "_index"},
            new String[] {"string", "double"},
            "id0,1.0");
    assertEquals(expectedBody, Util.toStringFromGZip(mockWebServer.takeRequest()));
  }

  @Test
  public void testAllEmbulkTypes() throws IOException, InterruptedException {
    testSuccessRun(
        newDefaultConfigSource(mockWebServer).set("action_type", "insert"),
        new Column[] {
          new Column(0, "id", Types.STRING),
          new Column(1, "long", Types.LONG),
          new Column(2, "double", Types.DOUBLE),
          new Column(3, "boolean", Types.BOOLEAN),
          new Column(4, "timestamp", Types.TIMESTAMP),
          new Column(5, "json", Types.JSON),
        },
        new String[] {
          "id0,100,0,true,2000-01-01 00:00:00.000000 +09:00,{\"k0\":\"key1\"}",
          "id1,-200,1,false,2001-02-03 04:05:06.700000 +00:00,{\"k0\":[0]}",
        },
        new String[] {"string", "double", "double", "boolean", "dateTime", "string"},
        new String[] {
          "id0,100.0,0.0,true,1999-12-31T15:00:00.000Z,{\"k0\":\"key1\"}",
          "id1,-200.0,1.0,false,2001-02-03T04:05:06.700Z,{\"k0\":[0]}"
        });
  }

  @Test
  public void testBatchSizeZero() {
    ConfigSource config =
        newDefaultConfigSource(mockWebServer).set("action_type", "insert").set("batch_size", 0);
    PartialExecutionException e =
        assertThrows(
            PartialExecutionException.class,
            () ->
                embulk.runOutput(
                    config, Util.createInputFile(testFolder, "id:string", "id0").toPath()));
    assertEquals(ConfigException.class, e.getCause().getClass());
  }

  @Test
  public void testBatchSizeOver200() {
    ConfigSource config =
        newDefaultConfigSource(mockWebServer).set("action_type", "insert").set("batch_size", 201);
    PartialExecutionException e =
        assertThrows(
            PartialExecutionException.class,
            () ->
                embulk.runOutput(
                    config, Util.createInputFile(testFolder, "id:string", "id0").toPath()));
    assertEquals(ConfigException.class, e.getCause().getClass());
  }

  @Test
  public void testBatchSizeCustom() throws IOException, InterruptedException {
    ConfigSource config =
        newDefaultConfigSource(mockWebServer).set("action_type", "insert").set("batch_size", 1);
    PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);

    // 2 records with batch_size=1 → 2 API calls
    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(Util.mockActionSuccessResponse(task.getActionType(), 1));
    mockWebServer.enqueue(Util.mockActionSuccessResponse(task.getActionType(), 1));
    File in = Util.createInputFile(testFolder, "id:string", "id0", "id1");
    embulk.runOutput(config, in.toPath());

    // login + 2 action calls; no logout
    assertEquals(3, mockWebServer.getRequestCount());
  }

  // ========== Association: validation tests ==========

  @Test
  public void testAssociationSourceColumnNotInSchema() {
    Map<String, String> assoc = new HashMap<>();
    assoc.put("reference_field", "AccountId");
    assoc.put("referenced_object", "Account");
    assoc.put("unique_key", "External_Id__c");
    assoc.put("source_column", "nonexistent_column");
    ConfigSource config =
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("associations", Arrays.asList(assoc));
    PartialExecutionException e =
        assertThrows(
            PartialExecutionException.class,
            () ->
                embulk.runOutput(
                    config, Util.createInputFile(testFolder, "id:string", "id0").toPath()));
    assertEquals(ConfigException.class, e.getCause().getClass());
  }

  @Test
  public void testAssociationDuplicateReferenceField() {
    Map<String, String> assoc1 = new HashMap<>();
    assoc1.put("reference_field", "AccountId");
    assoc1.put("referenced_object", "Account");
    assoc1.put("unique_key", "External_Id__c");
    assoc1.put("source_column", "code1");
    Map<String, String> assoc2 = new HashMap<>();
    assoc2.put("reference_field", "AccountId");
    assoc2.put("referenced_object", "Account");
    assoc2.put("unique_key", "External_Id__c");
    assoc2.put("source_column", "code2");
    ConfigSource config =
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("associations", Arrays.asList(assoc1, assoc2));
    PartialExecutionException e =
        assertThrows(
            PartialExecutionException.class,
            () ->
                embulk.runOutput(
                    config,
                    Util.createInputFile(
                            testFolder, "id:string,code1:string,code2:string", "id0,c1,c2")
                        .toPath()));
    assertEquals(ConfigException.class, e.getCause().getClass());
  }

  // ========== Association: empty associations (backward compatibility) ==========

  @Test
  public void testEmptyAssociationsBackwardCompat() throws IOException, InterruptedException {
    // Explicitly set empty associations — should behave identically to no associations
    testSuccessRun(
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("associations", Arrays.asList()),
        new Column[] {
          new Column(0, "id", Types.STRING), new Column(1, "test", Types.STRING),
        },
        new String[] {"id0,test0"},
        new String[] {"string", "string"},
        new String[] {"id0,test0"});
  }

  // ========== Association: single association insert ==========

  @Test
  public void testAssociationInsertNestedSObject() throws IOException, InterruptedException {
    Map<String, String> assoc = new HashMap<>();
    assoc.put("reference_field", "AccountId");
    assoc.put("referenced_object", "Account");
    assoc.put("unique_key", "External_Id__c");
    assoc.put("source_column", "account_code");
    ConfigSource config =
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("associations", Arrays.asList(assoc));

    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(Util.mockActionSuccessResponse("insert", 1));
    File in =
        Util.createInputFile(testFolder, "Name:string,account_code:string", "John Doe,COMP-001");
    embulk.runOutput(config, in.toPath());

    assertEquals(2, mockWebServer.getRequestCount());
    mockWebServer.takeRequest(); // skip login
    String body = Util.toStringFromGZip(mockWebServer.takeRequest());

    // Verify nested SObject is present with correct structure
    assertTrue(body.contains("<sobj:Account>"));
    assertTrue(body.contains("<sobj:type xsi:type=\"xsd:string\">Account</sobj:type>"));
    assertTrue(
        body.contains(
            "<sobj:External_Id__c xsi:type=\"xsd:string\">COMP-001</sobj:External_Id__c>"));
    assertTrue(body.contains("</sobj:Account>"));

    // Verify source_column value is NOT set as a direct field
    assertFalse(body.contains("<sobj:account_code"));

    // Verify normal fields are still present
    assertTrue(body.contains("<sobj:Name xsi:type=\"xsd:string\">John Doe</sobj:Name>"));
  }

  // ========== Association: multiple associations ==========

  @Test
  public void testAssociationMultipleNestedSObjects() throws IOException, InterruptedException {
    Map<String, String> assoc1 = new HashMap<>();
    assoc1.put("reference_field", "AccountId");
    assoc1.put("referenced_object", "Account");
    assoc1.put("unique_key", "External_Id__c");
    assoc1.put("source_column", "account_code");
    Map<String, String> assoc2 = new HashMap<>();
    assoc2.put("reference_field", "ReportsToId");
    assoc2.put("referenced_object", "Contact");
    assoc2.put("unique_key", "Email");
    assoc2.put("source_column", "manager_email");
    ConfigSource config =
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("associations", Arrays.asList(assoc1, assoc2));

    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(Util.mockActionSuccessResponse("insert", 1));
    File in =
        Util.createInputFile(
            testFolder,
            "Name:string,account_code:string,manager_email:string",
            "John Doe,COMP-001,boss@example.com");
    embulk.runOutput(config, in.toPath());

    mockWebServer.takeRequest();
    String body = Util.toStringFromGZip(mockWebServer.takeRequest());

    // First association: Account
    assertTrue(body.contains("<sobj:Account>"));
    assertTrue(
        body.contains(
            "<sobj:External_Id__c xsi:type=\"xsd:string\">COMP-001</sobj:External_Id__c>"));

    // Second association: ReportsTo
    assertTrue(body.contains("<sobj:ReportsTo>"));
    assertTrue(body.contains("<sobj:type xsi:type=\"xsd:string\">Contact</sobj:type>"));
    assertTrue(body.contains("<sobj:Email xsi:type=\"xsd:string\">boss@example.com</sobj:Email>"));

    // Source columns are NOT direct fields
    assertFalse(body.contains("<sobj:account_code"));
    assertFalse(body.contains("<sobj:manager_email"));
  }

  // ========== Association: null handling ==========

  @Test
  public void testAssociationNullSourceColumnIgnoreNullsFalse()
      throws IOException, InterruptedException {
    Map<String, String> assoc = new HashMap<>();
    assoc.put("reference_field", "AccountId");
    assoc.put("referenced_object", "Account");
    assoc.put("unique_key", "External_Id__c");
    assoc.put("source_column", "account_code");
    ConfigSource config =
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("ignore_nulls", false)
            .set("associations", Arrays.asList(assoc));

    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(Util.mockActionSuccessResponse("insert", 1));
    File in = Util.createInputFile(testFolder, "Name:string,account_code:string", "John Doe,");
    embulk.runOutput(config, in.toPath());

    mockWebServer.takeRequest();
    String body = Util.toStringFromGZip(mockWebServer.takeRequest());

    // No nested SObject should be present
    assertFalse(body.contains("<sobj:Account>"));

    // reference_field (AccountId) should be in fieldsToNull
    assertTrue(
        body.contains("<sobj:fieldsToNull xsi:type=\"xsd:string\">AccountId</sobj:fieldsToNull>"));
  }

  @Test
  public void testAssociationNullSourceColumnIgnoreNullsTrue()
      throws IOException, InterruptedException {
    Map<String, String> assoc = new HashMap<>();
    assoc.put("reference_field", "AccountId");
    assoc.put("referenced_object", "Account");
    assoc.put("unique_key", "External_Id__c");
    assoc.put("source_column", "account_code");
    ConfigSource config =
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("ignore_nulls", true)
            .set("associations", Arrays.asList(assoc));

    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(Util.mockActionSuccessResponse("insert", 1));
    File in = Util.createInputFile(testFolder, "Name:string,account_code:string", "John Doe,");
    embulk.runOutput(config, in.toPath());

    mockWebServer.takeRequest();
    String body = Util.toStringFromGZip(mockWebServer.takeRequest());

    // No nested SObject
    assertFalse(body.contains("<sobj:Account>"));
    // No fieldsToNull for AccountId (ignore_nulls=true → preserve existing reference)
    assertFalse(body.contains("AccountId"));
    // No fieldsToNull at all (ignore_nulls=true)
    assertFalse(body.contains("fieldsToNull"));
  }

  // ========== Association: mixed null and non-null in multiple associations ==========

  @Test
  public void testAssociationMixedNullAndNonNull() throws IOException, InterruptedException {
    Map<String, String> assoc1 = new HashMap<>();
    assoc1.put("reference_field", "AccountId");
    assoc1.put("referenced_object", "Account");
    assoc1.put("unique_key", "External_Id__c");
    assoc1.put("source_column", "account_code");
    Map<String, String> assoc2 = new HashMap<>();
    assoc2.put("reference_field", "ReportsToId");
    assoc2.put("referenced_object", "Contact");
    assoc2.put("unique_key", "Email");
    assoc2.put("source_column", "manager_email");
    ConfigSource config =
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("ignore_nulls", false)
            .set("associations", Arrays.asList(assoc1, assoc2));

    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(Util.mockActionSuccessResponse("insert", 1));
    // account_code has value, manager_email is null
    File in =
        Util.createInputFile(
            testFolder,
            "Name:string,account_code:string,manager_email:string",
            "John Doe,COMP-001,");
    embulk.runOutput(config, in.toPath());

    mockWebServer.takeRequest();
    String body = Util.toStringFromGZip(mockWebServer.takeRequest());

    // First association (non-null) → nested SObject
    assertTrue(body.contains("<sobj:Account>"));
    assertTrue(
        body.contains(
            "<sobj:External_Id__c xsi:type=\"xsd:string\">COMP-001</sobj:External_Id__c>"));

    // Second association (null) → fieldsToNull with ReportsToId
    assertFalse(body.contains("<sobj:ReportsTo>"));
    assertTrue(
        body.contains(
            "<sobj:fieldsToNull xsi:type=\"xsd:string\">ReportsToId</sobj:fieldsToNull>"));
  }

  // ========== Association: with different action types ==========

  @Test
  public void testAssociationWithUpsert() throws IOException, InterruptedException {
    Map<String, String> assoc = new HashMap<>();
    assoc.put("reference_field", "AccountId");
    assoc.put("referenced_object", "Account");
    assoc.put("unique_key", "External_Id__c");
    assoc.put("source_column", "account_code");
    ConfigSource config =
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "upsert")
            .set("associations", Arrays.asList(assoc));

    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(Util.mockActionSuccessResponse("upsert", 1));
    File in =
        Util.createInputFile(testFolder, "Name:string,account_code:string", "John Doe,COMP-001");
    embulk.runOutput(config, in.toPath());

    mockWebServer.takeRequest();
    String body = Util.toStringFromGZip(mockWebServer.takeRequest());

    // Verify it's an upsert request
    assertTrue(body.contains("m:upsert"));
    assertTrue(body.contains("<m:externalIDFieldName>key</m:externalIDFieldName>"));

    // Nested SObject is present
    assertTrue(body.contains("<sobj:Account>"));
    assertTrue(
        body.contains(
            "<sobj:External_Id__c xsi:type=\"xsd:string\">COMP-001</sobj:External_Id__c>"));
  }

  @Test
  public void testAssociationWithUpdate() throws IOException, InterruptedException {
    Map<String, String> assoc = new HashMap<>();
    assoc.put("reference_field", "AccountId");
    assoc.put("referenced_object", "Account");
    assoc.put("unique_key", "External_Id__c");
    assoc.put("source_column", "account_code");
    ConfigSource config =
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "update")
            .set("associations", Arrays.asList(assoc));

    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(Util.mockActionSuccessResponse("update", 1));
    File in =
        Util.createInputFile(testFolder, "Name:string,account_code:string", "John Doe,COMP-001");
    embulk.runOutput(config, in.toPath());

    mockWebServer.takeRequest();
    String body = Util.toStringFromGZip(mockWebServer.takeRequest());

    // Verify it's an update request
    assertTrue(body.contains("m:update"));

    // Nested SObject is present
    assertTrue(body.contains("<sobj:Account>"));
  }

  // ========== Association: custom field (relationship_name derivation __c → __r) ==========

  @Test
  public void testAssociationCustomField() throws IOException, InterruptedException {
    Map<String, String> assoc = new HashMap<>();
    assoc.put("reference_field", "Company__c");
    assoc.put("referenced_object", "TestCompany__c");
    assoc.put("unique_key", "Company_Code__c");
    assoc.put("source_column", "company_code");
    ConfigSource config =
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("associations", Arrays.asList(assoc));

    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(Util.mockActionSuccessResponse("insert", 1));
    File in =
        Util.createInputFile(testFolder, "Name:string,company_code:string", "John Doe,COMP-001");
    embulk.runOutput(config, in.toPath());

    mockWebServer.takeRequest();
    String body = Util.toStringFromGZip(mockWebServer.takeRequest());

    // Company__c → Company__r (relationship_name)
    assertTrue(body.contains("<sobj:Company__r>"));
    assertTrue(body.contains("<sobj:type xsi:type=\"xsd:string\">TestCompany__c</sobj:type>"));
    assertTrue(
        body.contains(
            "<sobj:Company_Code__c xsi:type=\"xsd:string\">COMP-001</sobj:Company_Code__c>"));
    assertTrue(body.contains("</sobj:Company__r>"));
  }

  // ========== Association: multiple records in single batch ==========

  @Test
  public void testAssociationMultipleRecords() throws IOException, InterruptedException {
    Map<String, String> assoc = new HashMap<>();
    assoc.put("reference_field", "AccountId");
    assoc.put("referenced_object", "Account");
    assoc.put("unique_key", "External_Id__c");
    assoc.put("source_column", "account_code");
    ConfigSource config =
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("associations", Arrays.asList(assoc));

    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(Util.mockActionSuccessResponse("insert", 3));
    File in =
        Util.createInputFile(
            testFolder,
            "Name:string,account_code:string",
            "Alice,COMP-001",
            "Bob,COMP-002",
            "Charlie,COMP-001");
    embulk.runOutput(config, in.toPath());

    mockWebServer.takeRequest();
    String body = Util.toStringFromGZip(mockWebServer.takeRequest());

    // 3 sObjects in the request
    assertEquals(3, countOccurrences(body, "<m:sObjects>"));

    // All have nested Account
    assertEquals(3, countOccurrences(body, "<sobj:Account>"));

    // Verify individual values
    assertTrue(body.contains("COMP-001"));
    assertTrue(body.contains("COMP-002"));
  }

  // ========== Association: non-string source_column type (long) ==========

  @Test
  public void testAssociationSourceColumnLongType() throws IOException, InterruptedException {
    Map<String, String> assoc = new HashMap<>();
    assoc.put("reference_field", "AccountId");
    assoc.put("referenced_object", "Account");
    assoc.put("unique_key", "External_Number__c");
    assoc.put("source_column", "account_number");
    ConfigSource config =
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("associations", Arrays.asList(assoc));

    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(Util.mockActionSuccessResponse("insert", 1));
    File in = Util.createInputFile(testFolder, "Name:string,account_number:long", "John Doe,12345");
    embulk.runOutput(config, in.toPath());

    mockWebServer.takeRequest();
    String body = Util.toStringFromGZip(mockWebServer.takeRequest());

    // Long value is converted to string for the nested SObject
    assertTrue(body.contains("<sobj:Account>"));
    assertTrue(
        body.contains(
            "<sobj:External_Number__c xsi:type=\"xsd:string\">12345</sobj:External_Number__c>"));

    // source_column is NOT set as a direct field
    assertFalse(body.contains("<sobj:account_number"));
  }

  // ========== Association: same source_column used by multiple associations ==========

  @Test
  public void testAssociationSameSourceColumnMultipleAssociations()
      throws IOException, InterruptedException {
    // Same source_column "code" is used by two different associations
    Map<String, String> assoc1 = new HashMap<>();
    assoc1.put("reference_field", "AccountId");
    assoc1.put("referenced_object", "Account");
    assoc1.put("unique_key", "External_Id__c");
    assoc1.put("source_column", "code");
    Map<String, String> assoc2 = new HashMap<>();
    assoc2.put("reference_field", "My_Custom__c");
    assoc2.put("referenced_object", "CustomObj__c");
    assoc2.put("unique_key", "Code__c");
    assoc2.put("source_column", "code");
    ConfigSource config =
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("associations", Arrays.asList(assoc1, assoc2));

    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(Util.mockActionSuccessResponse("insert", 1));
    File in = Util.createInputFile(testFolder, "Name:string,code:string", "John Doe,SHARED-001");
    embulk.runOutput(config, in.toPath());

    mockWebServer.takeRequest();
    String body = Util.toStringFromGZip(mockWebServer.takeRequest());

    // Both associations create nested SObjects from the same source value
    assertTrue(body.contains("<sobj:Account>"));
    assertTrue(body.contains("<sobj:My_Custom__r>"));
    // Both use the same value
    assertEquals(2, countOccurrences(body, "SHARED-001"));
  }

  // ========== Association: null handling with mixed normal null fields ==========

  @Test
  public void testAssociationNullWithOtherNullFieldsIgnoreNullsFalse()
      throws IOException, InterruptedException {
    Map<String, String> assoc = new HashMap<>();
    assoc.put("reference_field", "AccountId");
    assoc.put("referenced_object", "Account");
    assoc.put("unique_key", "External_Id__c");
    assoc.put("source_column", "account_code");
    ConfigSource config =
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("ignore_nulls", false)
            .set("associations", Arrays.asList(assoc));

    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(Util.mockActionSuccessResponse("insert", 1));
    // Both Name and account_code are null
    File in = Util.createInputFile(testFolder, "Name:string,account_code:string", ",");
    embulk.runOutput(config, in.toPath());

    mockWebServer.takeRequest();
    String body = Util.toStringFromGZip(mockWebServer.takeRequest());

    // Name should be in fieldsToNull (normal null field)
    assertTrue(
        body.contains("<sobj:fieldsToNull xsi:type=\"xsd:string\">Name</sobj:fieldsToNull>"));
    // AccountId should also be in fieldsToNull (association null)
    assertTrue(
        body.contains("<sobj:fieldsToNull xsi:type=\"xsd:string\">AccountId</sobj:fieldsToNull>"));
    // No nested SObject
    assertFalse(body.contains("<sobj:Account>"));
  }

  // ========== Association: records across multiple batches ==========

  @Test
  public void testAssociationWithBatchSize() throws IOException, InterruptedException {
    Map<String, String> assoc = new HashMap<>();
    assoc.put("reference_field", "AccountId");
    assoc.put("referenced_object", "Account");
    assoc.put("unique_key", "External_Id__c");
    assoc.put("source_column", "account_code");
    ConfigSource config =
        newDefaultConfigSource(mockWebServer)
            .set("action_type", "insert")
            .set("batch_size", 1)
            .set("associations", Arrays.asList(assoc));

    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(Util.mockActionSuccessResponse("insert", 1));
    mockWebServer.enqueue(Util.mockActionSuccessResponse("insert", 1));
    File in =
        Util.createInputFile(
            testFolder, "Name:string,account_code:string", "Alice,COMP-001", "Bob,COMP-002");
    embulk.runOutput(config, in.toPath());

    // login + 2 batches
    assertEquals(3, mockWebServer.getRequestCount());

    mockWebServer.takeRequest(); // login

    String body1 = Util.toStringFromGZip(mockWebServer.takeRequest());
    assertTrue(body1.contains("Alice"));
    assertTrue(body1.contains("COMP-001"));
    assertTrue(body1.contains("<sobj:Account>"));

    String body2 = Util.toStringFromGZip(mockWebServer.takeRequest());
    assertTrue(body2.contains("Bob"));
    assertTrue(body2.contains("COMP-002"));
    assertTrue(body2.contains("<sobj:Account>"));
  }

  // ========== Helper methods ==========

  private void testSuccessRun(String actionType) throws IOException, InterruptedException {
    testSuccessRun(
        newDefaultConfigSource(mockWebServer).set("action_type", actionType),
        new Column[] {
          new Column(0, "id", Types.STRING), new Column(1, "test", Types.STRING),
        },
        new String[] {"id0,test0", "id1,test1"},
        new String[] {"string", "string"},
        new String[] {"id0,test0", "id1,test1"});
  }

  public void testSuccessRun(
      ConfigSource config,
      Column[] columns,
      String[] inputs,
      String[] outputTypes,
      String[] outputValues)
      throws IOException, InterruptedException {
    testSuccessRun(
        config,
        columns,
        inputs,
        concat(Arrays.stream(columns).map(Column::getName).toArray(String[]::new), "_index"),
        concat(outputTypes, "double"),
        Arrays.stream(outputValues).map(x -> x + ",1.0").toArray(String[]::new));
  }

  public void testSuccessRun(
      ConfigSource config,
      Column[] columns,
      String[] inputs,
      String[] outputNames,
      String[] outputTypes,
      String[] outputValues)
      throws IOException, InterruptedException {
    PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);

    MockResponse mockActionResponse =
        Util.mockActionSuccessResponse(task.getActionType(), inputs.length);

    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(mockActionResponse);
    String header =
        Arrays.stream(columns)
            .map(x -> String.format("%s:%s", x.getName(), x.getType()))
            .collect(Collectors.joining(","));
    File in = Util.createInputFile(testFolder, header, inputs);
    embulk.runOutput(config, in.toPath());

    // login + action only; no logout call
    assertEquals(2, mockWebServer.getRequestCount());
    assertEquals(
        Util.readResource("loginRequestBody.xml"),
        Util.toStringFromGZip(mockWebServer.takeRequest()));
    String expectedBody =
        Util.actionRequestBody(task.getActionType(), outputNames, outputTypes, outputValues);
    assertEquals(expectedBody, Util.toStringFromGZip(mockWebServer.takeRequest()));
  }

  private static String[] concat(String[] src, String elem) {
    List<String> list = new ArrayList<>(Arrays.asList(src));
    list.add(elem);
    return list.toArray(new String[0]);
  }

  private static int countOccurrences(String str, String sub) {
    int count = 0;
    int idx = 0;
    while ((idx = str.indexOf(sub, idx)) != -1) {
      count++;
      idx += sub.length();
    }
    return count;
  }
}
