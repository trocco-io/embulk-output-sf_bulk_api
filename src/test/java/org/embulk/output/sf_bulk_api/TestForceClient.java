package org.embulk.output.sf_bulk_api;

import static org.embulk.output.sf_bulk_api.Util.actionRequestBody;
import static org.embulk.output.sf_bulk_api.Util.mockActionSuccessResponse;
import static org.embulk.output.sf_bulk_api.Util.mockResponse;
import static org.embulk.output.sf_bulk_api.Util.readResource;
import static org.embulk.output.sf_bulk_api.Util.toStringFromGZip;
import static org.junit.Assert.assertEquals;

import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import okhttp3.mockwebserver.MockWebServer;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Schema;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestForceClient {
  protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

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

  @Test
  public void testInsert() throws ConnectionException, InterruptedException, IOException {
    testAction("insert");
  }

  @Test
  public void testUpdate() throws ConnectionException, InterruptedException, IOException {
    testAction("update");
  }

  @Test
  public void testUpsert() throws ConnectionException, InterruptedException, IOException {
    testAction("upsert");
  }

  @Test
  public void testDelete() throws ConnectionException, InterruptedException, IOException {
    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(mockActionSuccessResponse("delete", 2));

    newDeleteForceClient("id").action(newRecords(2));

    assertEquals(2, mockWebServer.getRequestCount());
    assertEquals(
        readResource("loginRequestBody.xml"), toStringFromGZip(mockWebServer.takeRequest()));
    assertEquals(
        Util.deleteRequestBody("id0", "id1"), toStringFromGZip(mockWebServer.takeRequest()));
  }

  @Test
  public void testDeleteSkipsNullKey()
      throws ConnectionException, InterruptedException, IOException {
    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    // Only the record with a non-null delete_key value is sent to delete().
    mockWebServer.enqueue(mockActionSuccessResponse("delete", 1));

    List<SObject> records = new ArrayList<>();
    SObject withId = new SObject(Util.OBJECT);
    withId.addField("id", "id0");
    records.add(withId);
    SObject withoutId = new SObject(Util.OBJECT);
    withoutId.addField("test", "no_id_here");
    records.add(withoutId);

    long failures = newDeleteForceClient("id").action(records);

    // The null-key record is counted as a failure without an API call row.
    assertEquals(1, failures);
    assertEquals(2, mockWebServer.getRequestCount());
    assertEquals(
        readResource("loginRequestBody.xml"), toStringFromGZip(mockWebServer.takeRequest()));
    assertEquals(Util.deleteRequestBody("id0"), toStringFromGZip(mockWebServer.takeRequest()));
  }

  @Test
  public void testNoLogoutCallOnClose()
      throws ConnectionException, InterruptedException, IOException {
    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(mockActionSuccessResponse("insert", 2));

    ForceClient forceClient = newForceClient("insert");
    forceClient.action(newRecords(2));

    // Only login + action requests should be made; no logout request
    assertEquals(2, mockWebServer.getRequestCount());
    assertEquals(
        readResource("loginRequestBody.xml"), toStringFromGZip(mockWebServer.takeRequest()));
  }

  private void testAction(String actionType)
      throws ConnectionException, InterruptedException, IOException {
    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(mockActionSuccessResponse(actionType, 2));

    newForceClient(actionType).action(newRecords(2));

    assertEquals(2, mockWebServer.getRequestCount());
    assertEquals(
        readResource("loginRequestBody.xml"), toStringFromGZip(mockWebServer.takeRequest()));
    assertEquals(
        actionRequestBody(
            actionType,
            new String[] {"id", "test"},
            new String[] {"string", "string"},
            "id0,test0",
            "id1,test1"),
        toStringFromGZip(mockWebServer.takeRequest()));
  }

  private ForceClient newForceClient(String actionType) throws ConnectionException {
    ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
    ConfigSource config = Util.newDefaultConfigSource(mockWebServer).set("action_type", actionType);
    PluginTask task = configMapper.map(config, PluginTask.class);

    Schema schema = new Schema(Collections.emptyList());
    return new ForceClient(task, new ErrorHandler(schema));
  }

  private ForceClient newDeleteForceClient(String deleteKey) throws ConnectionException {
    ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
    ConfigSource config =
        Util.newDefaultConfigSource(mockWebServer)
            .set("action_type", "delete")
            .set("delete_key", deleteKey);
    PluginTask task = configMapper.map(config, PluginTask.class);

    Schema schema = new Schema(Collections.emptyList());
    return new ForceClient(task, new ErrorHandler(schema));
  }

  private List<SObject> newRecords(int count) {
    List<SObject> records = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      SObject record = new SObject(Util.OBJECT);
      record.addField("id", String.format("id%d", i));
      record.addField("test", String.format("test%d", i));
      records.add(record);
    }
    return records;
  }
}
