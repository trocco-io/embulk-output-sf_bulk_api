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
  public void testLogout() throws ConnectionException, InterruptedException, IOException {
    mockWebServer.enqueue(mockResponse("loginResponseBody.xml"));
    mockWebServer.enqueue(mockResponse("logoutResponseBody.xml"));

    newForceClient("update").logout();

    assertEquals(2, mockWebServer.getRequestCount());
    assertEquals(
        readResource("loginRequestBody.xml"), toStringFromGZip(mockWebServer.takeRequest()));
    assertEquals(
        readResource("logoutRequestBody.xml"), toStringFromGZip(mockWebServer.takeRequest()));
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
