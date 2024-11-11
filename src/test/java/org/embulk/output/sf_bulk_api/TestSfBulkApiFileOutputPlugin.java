package org.embulk.output.sf_bulk_api;

import static org.embulk.output.sf_bulk_api.Util.mockActionResponse;
import static org.embulk.output.sf_bulk_api.Util.mockResponse;
import static org.embulk.output.sf_bulk_api.Util.newDefaultConfigSource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
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
    for (int i = 0; i < 2; i++) {
      mockWebServer.enqueue(mockResponse("logoutResponseBody.xml"));
    }
    File in = Util.createInputFile(testFolder, "id:string", "id0");
    if (throwIfFailed) {
      assertThrows(PartialExecutionException.class, () -> embulk.runOutput(config, in.toPath()));
    } else {
      embulk.runOutput(config, in.toPath());
    }
    assertEquals(4, mockWebServer.getRequestCount());
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
    for (int i = 0; i < 2; i++) {
      assertEquals(
          Util.readResource("logoutRequestBody.xml"),
          Util.toStringFromGZip(mockWebServer.takeRequest()));
    }
  }

  @Test
  public void testAllEmbulkTypes() throws IOException, InterruptedException {
    // NOTE: If the type is Json, an input value is ignored. The id value is used.
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
          "id0,100.0,0.0,true,1999-12-31T15:00:00.000Z,id0",
          "id1,-200.0,1.0,false,2001-02-03T04:05:06.700Z,id1"
        });
  }

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
    for (int i = 0; i < 2; i++) {
      mockWebServer.enqueue(mockResponse("logoutResponseBody.xml"));
    }
    String header =
        Arrays.stream(columns)
            .map(x -> String.format("%s:%s", x.getName(), x.getType()))
            .collect(Collectors.joining(","));
    File in = Util.createInputFile(testFolder, header, inputs);
    embulk.runOutput(config, in.toPath());

    assertEquals(4, mockWebServer.getRequestCount());
    assertEquals(
        Util.readResource("loginRequestBody.xml"),
        Util.toStringFromGZip(mockWebServer.takeRequest()));
    String expectedBody =
        Util.actionRequestBody(task.getActionType(), outputNames, outputTypes, outputValues);
    assertEquals(expectedBody, Util.toStringFromGZip(mockWebServer.takeRequest()));
    for (int i = 0; i < 2; i++) {
      assertEquals(
          Util.readResource("logoutRequestBody.xml"),
          Util.toStringFromGZip(mockWebServer.takeRequest()));
    }
  }

  private static String[] concat(String[] src, String elem) {
    List<String> list = new ArrayList<>(Arrays.asList(src));
    list.add(elem);
    return list.toArray(new String[0]);
  }
}
