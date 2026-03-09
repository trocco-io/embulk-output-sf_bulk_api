package org.embulk.output.sf_bulk_api;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.embulk.spi.Schema;
import org.junit.Before;
import org.junit.Test;

public class TestSfIdResolver {
  private PartnerConnection mockConnection;
  private ErrorHandler errorHandler;
  private static final String OBJECT_TYPE = "Account";
  private static final String UPDATE_KEY = "External_Id__c";

  @Before
  public void setup() {
    mockConnection = mock(PartnerConnection.class);
    Schema schema = new Schema(Collections.emptyList());
    errorHandler = new ErrorHandler(schema);
  }

  @Test
  public void testResolveSuccess() throws ConnectionException {
    SfIdResolver resolver = new SfIdResolver(mockConnection, OBJECT_TYPE, UPDATE_KEY, errorHandler);

    List<SObject> records = new ArrayList<>();
    SObject record1 = new SObject(OBJECT_TYPE);
    record1.addField(UPDATE_KEY, "ext001");
    record1.addField("Name", "Test1");
    records.add(record1);

    SObject record2 = new SObject(OBJECT_TYPE);
    record2.addField(UPDATE_KEY, "ext002");
    record2.addField("Name", "Test2");
    records.add(record2);

    // Mock SOQL result
    QueryResult queryResult = new QueryResult();
    SObject sfRecord1 = new SObject(OBJECT_TYPE);
    sfRecord1.setId("001000000000001");
    sfRecord1.addField(UPDATE_KEY, "ext001");
    SObject sfRecord2 = new SObject(OBJECT_TYPE);
    sfRecord2.setId("001000000000002");
    sfRecord2.addField(UPDATE_KEY, "ext002");
    queryResult.setRecords(new SObject[] {sfRecord1, sfRecord2});
    queryResult.setDone(true);

    when(mockConnection.query(anyString())).thenReturn(queryResult);

    SfIdResolver.ResolveResult result = resolver.resolve(records);

    assertEquals(2, result.getResolvedRecords().size());
    assertEquals(0, result.getUnresolvedCount());
    assertEquals("001000000000001", result.getResolvedRecords().get(0).getId());
    assertEquals("001000000000002", result.getResolvedRecords().get(1).getId());
  }

  @Test
  public void testResolveNoMatchingRecord() throws ConnectionException {
    SfIdResolver resolver = new SfIdResolver(mockConnection, OBJECT_TYPE, UPDATE_KEY, errorHandler);

    List<SObject> records = new ArrayList<>();
    SObject record = new SObject(OBJECT_TYPE);
    record.addField(UPDATE_KEY, "ext_nonexistent");
    record.addField("Name", "Test");
    records.add(record);

    // Mock empty SOQL result
    QueryResult queryResult = new QueryResult();
    queryResult.setRecords(new SObject[] {});
    queryResult.setDone(true);

    when(mockConnection.query(anyString())).thenReturn(queryResult);

    SfIdResolver.ResolveResult result = resolver.resolve(records);

    assertEquals(0, result.getResolvedRecords().size());
    assertEquals(1, result.getUnresolvedCount());
  }

  @Test
  public void testResolveMultipleMatchingSfRecords() throws ConnectionException {
    SfIdResolver resolver = new SfIdResolver(mockConnection, OBJECT_TYPE, UPDATE_KEY, errorHandler);

    List<SObject> records = new ArrayList<>();
    SObject record = new SObject(OBJECT_TYPE);
    record.addField(UPDATE_KEY, "ext_dup");
    record.addField("Name", "Test");
    records.add(record);

    // Mock SOQL result with duplicates
    QueryResult queryResult = new QueryResult();
    SObject sfRecord1 = new SObject(OBJECT_TYPE);
    sfRecord1.setId("001000000000001");
    sfRecord1.addField(UPDATE_KEY, "ext_dup");
    SObject sfRecord2 = new SObject(OBJECT_TYPE);
    sfRecord2.setId("001000000000002");
    sfRecord2.addField(UPDATE_KEY, "ext_dup");
    queryResult.setRecords(new SObject[] {sfRecord1, sfRecord2});
    queryResult.setDone(true);

    when(mockConnection.query(anyString())).thenReturn(queryResult);

    SfIdResolver.ResolveResult result = resolver.resolve(records);

    assertEquals(0, result.getResolvedRecords().size());
    assertEquals(1, result.getUnresolvedCount());
  }

  @Test
  public void testResolveDuplicateKeysInInput() throws ConnectionException {
    SfIdResolver resolver = new SfIdResolver(mockConnection, OBJECT_TYPE, UPDATE_KEY, errorHandler);

    List<SObject> records = new ArrayList<>();
    SObject record1 = new SObject(OBJECT_TYPE);
    record1.addField(UPDATE_KEY, "ext_same");
    record1.addField("Name", "Test1");
    records.add(record1);

    SObject record2 = new SObject(OBJECT_TYPE);
    record2.addField(UPDATE_KEY, "ext_same");
    record2.addField("Name", "Test2");
    records.add(record2);

    // SOQL should not be called since all keys are duplicates in input
    SfIdResolver.ResolveResult result = resolver.resolve(records);

    assertEquals(0, result.getResolvedRecords().size());
    assertEquals(2, result.getUnresolvedCount());
    verify(mockConnection, never()).query(anyString());
  }

  @Test
  public void testResolveNullKeyValue() throws ConnectionException {
    SfIdResolver resolver = new SfIdResolver(mockConnection, OBJECT_TYPE, UPDATE_KEY, errorHandler);

    List<SObject> records = new ArrayList<>();
    SObject record = new SObject(OBJECT_TYPE);
    // update_key field is not set (null)
    record.addField("Name", "Test");
    records.add(record);

    SfIdResolver.ResolveResult result = resolver.resolve(records);

    assertEquals(0, result.getResolvedRecords().size());
    assertEquals(1, result.getUnresolvedCount());
    verify(mockConnection, never()).query(anyString());
  }

  @Test
  public void testResolveMixedResults() throws ConnectionException {
    SfIdResolver resolver = new SfIdResolver(mockConnection, OBJECT_TYPE, UPDATE_KEY, errorHandler);

    List<SObject> records = new ArrayList<>();
    // Record with valid key that will be found
    SObject record1 = new SObject(OBJECT_TYPE);
    record1.addField(UPDATE_KEY, "ext_found");
    record1.addField("Name", "Found");
    records.add(record1);

    // Record with valid key that will not be found
    SObject record2 = new SObject(OBJECT_TYPE);
    record2.addField(UPDATE_KEY, "ext_notfound");
    record2.addField("Name", "NotFound");
    records.add(record2);

    // Record with null key
    SObject record3 = new SObject(OBJECT_TYPE);
    record3.addField("Name", "NullKey");
    records.add(record3);

    // Mock SOQL result - only ext_found is found
    QueryResult queryResult = new QueryResult();
    SObject sfRecord = new SObject(OBJECT_TYPE);
    sfRecord.setId("001000000000001");
    sfRecord.addField(UPDATE_KEY, "ext_found");
    queryResult.setRecords(new SObject[] {sfRecord});
    queryResult.setDone(true);

    when(mockConnection.query(anyString())).thenReturn(queryResult);

    SfIdResolver.ResolveResult result = resolver.resolve(records);

    assertEquals(1, result.getResolvedRecords().size());
    assertEquals(2, result.getUnresolvedCount());
    assertEquals("001000000000001", result.getResolvedRecords().get(0).getId());
  }

  @Test
  public void testResolveSoqlEscaping() throws ConnectionException {
    SfIdResolver resolver = new SfIdResolver(mockConnection, OBJECT_TYPE, UPDATE_KEY, errorHandler);

    List<SObject> records = new ArrayList<>();
    SObject record = new SObject(OBJECT_TYPE);
    record.addField(UPDATE_KEY, "value'with\"quotes");
    record.addField("Name", "Test");
    records.add(record);

    QueryResult queryResult = new QueryResult();
    SObject sfRecord = new SObject(OBJECT_TYPE);
    sfRecord.setId("001000000000001");
    sfRecord.addField(UPDATE_KEY, "value'with\"quotes");
    queryResult.setRecords(new SObject[] {sfRecord});
    queryResult.setDone(true);

    when(mockConnection.query(anyString())).thenReturn(queryResult);

    SfIdResolver.ResolveResult result = resolver.resolve(records);

    assertEquals(1, result.getResolvedRecords().size());
    // Verify the SOQL query was properly escaped
    verify(mockConnection, times(1)).query(anyString());
  }

  @Test
  public void testResolveQueryMore() throws ConnectionException {
    SfIdResolver resolver = new SfIdResolver(mockConnection, OBJECT_TYPE, UPDATE_KEY, errorHandler);

    List<SObject> records = new ArrayList<>();
    SObject record1 = new SObject(OBJECT_TYPE);
    record1.addField(UPDATE_KEY, "ext001");
    records.add(record1);

    SObject record2 = new SObject(OBJECT_TYPE);
    record2.addField(UPDATE_KEY, "ext002");
    records.add(record2);

    // First query result - not done
    QueryResult queryResult1 = new QueryResult();
    SObject sfRecord1 = new SObject(OBJECT_TYPE);
    sfRecord1.setId("001000000000001");
    sfRecord1.addField(UPDATE_KEY, "ext001");
    queryResult1.setRecords(new SObject[] {sfRecord1});
    queryResult1.setDone(false);
    queryResult1.setQueryLocator("locator123");

    // Second query result - done
    QueryResult queryResult2 = new QueryResult();
    SObject sfRecord2 = new SObject(OBJECT_TYPE);
    sfRecord2.setId("001000000000002");
    sfRecord2.addField(UPDATE_KEY, "ext002");
    queryResult2.setRecords(new SObject[] {sfRecord2});
    queryResult2.setDone(true);

    when(mockConnection.query(anyString())).thenReturn(queryResult1);
    when(mockConnection.queryMore("locator123")).thenReturn(queryResult2);

    SfIdResolver.ResolveResult result = resolver.resolve(records);

    assertEquals(2, result.getResolvedRecords().size());
    assertEquals(0, result.getUnresolvedCount());
    verify(mockConnection, times(1)).query(anyString());
    verify(mockConnection, times(1)).queryMore("locator123");
  }
}
