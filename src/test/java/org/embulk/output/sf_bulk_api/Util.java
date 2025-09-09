package org.embulk.output.sf_bulk_api;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.embulk.config.ConfigSource;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.rules.TemporaryFolder;

public class Util {
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  public static final int SERVER_PORT = 18888;
  public static final String OBJECT = "object__c";

  public static ConfigSource newDefaultConfigSource(MockWebServer mockWebServer) {
    return CONFIG_MAPPER_FACTORY
        .newConfigSource()
        .set("type", "sf_bulk_api")
        .set("username", "username")
        .set("password", "password")
        .set("api_version", "46.0")
        .set("security_token", "security_token")
        .set("auth_end_point", mockWebServer.url("/services/Soap/u/").toString())
        .set("object", "object__c")
        .set("upsert_key", "key");
  }

  public static String insertRequestBody(
      String[] names, String[] types, String... commaSeparatedValues) {
    return replaceActionRequestBodyTemplate(
        "m:create", sobjectsRequestString(names, types, commaSeparatedValues));
  }

  public static String updateRequestBody(
      String[] names, String[] types, String... commaSeparatedValues) {
    return replaceActionRequestBodyTemplate(
        "m:update", sobjectsRequestString(names, types, commaSeparatedValues));
  }

  public static String upsertRequestBody(
      String[] names, String[] types, String... commaSeparatedValues) {
    return replaceActionRequestBodyTemplate(
        "m:upsert",
        "<m:externalIDFieldName>key</m:externalIDFieldName>"
            + sobjectsRequestString(names, types, commaSeparatedValues));
  }

  public static String actionRequestBody(
      String actionType, String[] names, String[] types, String... commaSeparatedValues) {
    if (actionType.equals("insert")) {
      return Util.insertRequestBody(names, types, commaSeparatedValues);
    } else if (actionType.equals("update")) {
      return Util.updateRequestBody(names, types, commaSeparatedValues);
    } else if (actionType.equals("upsert")) {
      return Util.upsertRequestBody(names, types, commaSeparatedValues);
    } else {
      throw new AssertionError();
    }
  }

  private static String replaceActionRequestBodyTemplate(String actionTypeTag, String actionBody) {
    return readResource("actionRequestBodyTemplate.xml")
        .replaceAll("##ACTION_TYPE_TAG##", actionTypeTag)
        .replaceAll("##ACTION_BODY##", actionBody);
  }

  private static String sobjectsRequestString(
      String[] names, String[] types, String... commaSeparatedValues) {
    return Arrays.stream(commaSeparatedValues)
        .map(
            commaSeparatedValue -> {
              String[] values = commaSeparatedValue.split(",");
              List<String> attrs = new ArrayList<>();
              attrs.add(String.format("<sobj:type xsi:type=\"xsd:string\">%s</sobj:type>", OBJECT));
              for (int i = 0; i < values.length; i++) {
                String v = values[i];
                String n = names[i];
                String t = types[i];
                attrs.add(String.format("<sobj:%s xsi:type=\"xsd:%s\">%s</sobj:%s>", n, t, v, n));
              }
              return String.format("<m:sObjects>%s</m:sObjects>", String.join("", attrs));
            })
        .collect(Collectors.joining());
  }

  public static MockResponse mockInsertResponse(Boolean[] results) {
    return replaceActionResponseBodyTemplate("createResponse", results);
  }

  public static MockResponse mockUpdateResponse(Boolean[] results) {
    return replaceActionResponseBodyTemplate("updateResponse", results);
  }

  public static MockResponse mockUpsertResponse(Boolean[] results) {
    return replaceActionResponseBodyTemplate("upsertResponse", results);
  }

  public static MockResponse mockActionResponse(String actionType, Boolean[] results) {
    if (actionType.equals("insert")) {
      return mockInsertResponse(results);
    } else if (actionType.equals("update")) {
      return mockUpdateResponse(results);
    } else if (actionType.equals("upsert")) {
      return mockUpsertResponse(results);
    } else {
      throw new AssertionError();
    }
  }

  public static MockResponse mockActionSuccessResponse(String actionType, int success) {
    Boolean[] results = IntStream.range(0, success).mapToObj(x -> true).toArray(Boolean[]::new);
    return mockActionResponse(actionType, results);
  }

  private static MockResponse replaceActionResponseBodyTemplate(
      String responseTag, Boolean[] results) {
    String resultElem = "<result><id>result_id</id><success>%s</success></result>";
    String resultsString =
        Arrays.stream(results).map(x -> String.format(resultElem, x)).collect(Collectors.joining());
    String body =
        readResource("actionResponseBodyTemplate.xml")
            .replaceAll("##RESULTS_BODY##", resultsString)
            .replaceAll("##RESPONSE_TAG##", responseTag);

    MockResponse mockResponse = new MockResponse();
    mockResponse.setBody(body);
    mockResponse.setResponseCode(200);
    return mockResponse;
  }

  public static MockResponse mockResponse(String resourceName) {
    MockResponse mockResponse = new MockResponse();
    mockResponse.setBody(Util.readResource(resourceName));
    mockResponse.setResponseCode(200);
    return mockResponse;
  }

  public static String readResource(String resourceName) {
    URL url = Objects.requireNonNull(Util.class.getClassLoader().getResource(resourceName));
    try {
      byte[] bytes = Files.readAllBytes(Paths.get(url.toURI()));
      return new String(bytes, StandardCharsets.UTF_8);
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static String toStringFromGZip(RecordedRequest request) throws IOException {
    try (InputStream gzippedResponse = new ByteArrayInputStream(request.getBody().readByteArray());
        InputStream ungzippedResponse = new GZIPInputStream(gzippedResponse);
        Reader reader = new InputStreamReader(ungzippedResponse, StandardCharsets.UTF_8);
        Writer writer = new StringWriter(); ) {
      char[] buffer = new char[10240];
      for (int length = 0; (length = reader.read(buffer)) > 0; ) {
        writer.write(buffer, 0, length);
      }
      return writer.toString();
    }
  }

  public static File createInputFile(TemporaryFolder testFolder, String header, String... data)
      throws IOException {
    File in = testFolder.newFile("embulk-output-sf_bulk_api-input.csv");
    // Remove hacking double column after merging https://github.com/embulk/embulk/pull/1476.
    List<String> contents =
        Stream.concat(Stream.of(header + ",_index:double"), Stream.of(data).map(s -> s + ",1.0"))
            .collect(Collectors.toList());
    Files.write(in.toPath(), contents);
    return in;
  }
}
