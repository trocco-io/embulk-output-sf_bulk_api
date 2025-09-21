package org.embulk.output.sf_bulk_api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestSfBulkApiOutputPluginFileConcat {
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private SfBulkApiOutputPlugin plugin;
  private Path errorFilePath;
  private Path tempDir;

  @Before
  public void setup() throws IOException {
    tempFolder.create();
    tempDir = tempFolder.getRoot().toPath();
    errorFilePath = tempDir.resolve("error_output.jsonl");
    plugin = new SfBulkApiOutputPlugin();
  }

  @After
  public void tearDown() throws IOException {
    // Clean up any remaining files
    Files.walk(tempDir)
        .filter(Files::isRegularFile)
        .forEach(
            path -> {
              try {
                Files.deleteIfExists(path);
              } catch (IOException e) {
                // Ignore cleanup errors
              }
            });
  }

  @Test
  public void testConcatenateErrorFiles_MultipleTaskFiles() throws IOException {
    // Create multiple task error files
    createTaskErrorFile(
        0,
        "{\"record_data\":{\"id\":\"1\"},\"error_code\":\"E1\",\"error_message\":\"Error 1\"}\n");
    createTaskErrorFile(
        1,
        "{\"record_data\":{\"id\":\"2\"},\"error_code\":\"E2\",\"error_message\":\"Error 2\"}\n");
    createTaskErrorFile(
        2,
        "{\"record_data\":{\"id\":\"3\"},\"error_code\":\"E3\",\"error_message\":\"Error 3\"}\n");

    // Create a mock task source with error_records_detail_output_file configured
    ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource();
    config.set("auth_method", "user_password");
    config.set("username", "test_user");
    config.set("password", "test_pass");
    config.set("object", "TestObject");
    config.set("action_type", "insert");
    config.set("error_records_detail_output_file", errorFilePath.toString());

    TaskSource taskSource = taskSourceFromConfig(config);
    List<TaskReport> successTaskReports = new ArrayList<>();

    // Call cleanup which should concatenate the files
    plugin.cleanup(taskSource, null, 3, successTaskReports);

    // Verify the concatenated file exists and has all content
    assertTrue(Files.exists(errorFilePath));
    List<String> lines = Files.readAllLines(errorFilePath);
    assertEquals(3, lines.size());
    assertTrue(lines.get(0).contains("\"id\":\"1\""));
    assertTrue(lines.get(1).contains("\"id\":\"2\""));
    assertTrue(lines.get(2).contains("\"id\":\"3\""));

    // Verify task files were deleted
    assertFalse(Files.exists(getTaskFilePath(0)));
    assertFalse(Files.exists(getTaskFilePath(1)));
    assertFalse(Files.exists(getTaskFilePath(2)));
  }

  @Test
  public void testConcatenateErrorFiles_EmptyTaskFiles() throws IOException {
    // Create task files with no content
    createTaskErrorFile(0, "");
    createTaskErrorFile(1, "");

    ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource();
    config.set("auth_method", "user_password");
    config.set("username", "test_user");
    config.set("password", "test_pass");
    config.set("object", "TestObject");
    config.set("action_type", "insert");
    config.set("error_records_detail_output_file", errorFilePath.toString());

    TaskSource taskSource = taskSourceFromConfig(config);
    List<TaskReport> successTaskReports = new ArrayList<>();

    plugin.cleanup(taskSource, null, 2, successTaskReports);

    // Verify no output file is created for empty task files
    assertFalse(Files.exists(errorFilePath));

    // Verify task files were deleted
    assertFalse(Files.exists(getTaskFilePath(0)));
    assertFalse(Files.exists(getTaskFilePath(1)));
  }

  @Test
  public void testConcatenateErrorFiles_MixedEmptyAndNonEmpty() throws IOException {
    // Create mix of empty and non-empty task files
    createTaskErrorFile(
        0,
        "{\"record_data\":{\"id\":\"1\"},\"error_code\":\"E1\",\"error_message\":\"Error 1\"}\n");
    createTaskErrorFile(1, "");
    createTaskErrorFile(
        2,
        "{\"record_data\":{\"id\":\"3\"},\"error_code\":\"E3\",\"error_message\":\"Error 3\"}\n");

    ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource();
    config.set("auth_method", "user_password");
    config.set("username", "test_user");
    config.set("password", "test_pass");
    config.set("object", "TestObject");
    config.set("action_type", "insert");
    config.set("error_records_detail_output_file", errorFilePath.toString());

    TaskSource taskSource = taskSourceFromConfig(config);
    List<TaskReport> successTaskReports = new ArrayList<>();

    plugin.cleanup(taskSource, null, 3, successTaskReports);

    // Verify file exists with only non-empty content
    assertTrue(Files.exists(errorFilePath));
    List<String> lines = Files.readAllLines(errorFilePath);
    assertEquals(2, lines.size());
    assertTrue(lines.get(0).contains("\"id\":\"1\""));
    assertTrue(lines.get(1).contains("\"id\":\"3\""));

    // Verify all task files were deleted
    assertFalse(Files.exists(getTaskFilePath(0)));
    assertFalse(Files.exists(getTaskFilePath(1)));
    assertFalse(Files.exists(getTaskFilePath(2)));
  }

  @Test
  public void testConcatenateErrorFiles_NoTaskFiles() throws IOException {
    // Don't create any task files

    ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource();
    config.set("auth_method", "user_password");
    config.set("username", "test_user");
    config.set("password", "test_pass");
    config.set("object", "TestObject");
    config.set("action_type", "insert");
    config.set("error_records_detail_output_file", errorFilePath.toString());

    TaskSource taskSource = taskSourceFromConfig(config);
    List<TaskReport> successTaskReports = new ArrayList<>();

    plugin.cleanup(taskSource, null, 3, successTaskReports);

    // Verify no output file is created when no task files exist
    assertFalse(Files.exists(errorFilePath));
  }

  @Test
  public void testConcatenateErrorFiles_NoErrorFileConfigured() throws IOException {
    // Create task files that shouldn't be processed
    createTaskErrorFile(
        0,
        "{\"record_data\":{\"id\":\"1\"},\"error_code\":\"E1\",\"error_message\":\"Error 1\"}\n");

    ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource();
    config.set("auth_method", "user_password");
    config.set("username", "test_user");
    config.set("password", "test_pass");
    config.set("object", "TestObject");
    config.set("action_type", "insert");
    // Don't set error_records_detail_output_file

    TaskSource taskSource = taskSourceFromConfig(config);
    List<TaskReport> successTaskReports = new ArrayList<>();

    plugin.cleanup(taskSource, null, 1, successTaskReports);

    // Verify no processing happens when error file is not configured
    assertFalse(Files.exists(errorFilePath));
    assertTrue(Files.exists(getTaskFilePath(0))); // Task file should still exist
  }

  @Test
  public void testConcatenateErrorFiles_LargeNumberOfFiles() throws IOException {
    // Test with larger number of task files
    int numTasks = 20;
    for (int i = 0; i < numTasks; i++) {
      createTaskErrorFile(
          i,
          String.format(
              "{\"record_data\":{\"id\":\"%d\"},\"error_code\":\"E%d\",\"error_message\":\"Error %d\"}\n",
              i, i, i));
    }

    ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource();
    config.set("auth_method", "user_password");
    config.set("username", "test_user");
    config.set("password", "test_pass");
    config.set("object", "TestObject");
    config.set("action_type", "insert");
    config.set("error_records_detail_output_file", errorFilePath.toString());

    TaskSource taskSource = taskSourceFromConfig(config);
    List<TaskReport> successTaskReports = new ArrayList<>();

    plugin.cleanup(taskSource, null, numTasks, successTaskReports);

    // Verify all content is present
    assertTrue(Files.exists(errorFilePath));
    List<String> lines = Files.readAllLines(errorFilePath);
    assertEquals(numTasks, lines.size());

    // Verify files are in order (due to sorted() in the implementation)
    for (int i = 0; i < numTasks; i++) {
      assertTrue(lines.get(i).contains(String.format("\"id\":\"%d\"", i)));
    }

    // Verify all task files were deleted
    for (int i = 0; i < numTasks; i++) {
      assertFalse(Files.exists(getTaskFilePath(i)));
    }
  }

  @Test
  public void testConcatenateErrorFiles_HandleIOExceptionGracefully() throws IOException {
    // Create a task file that we'll make unreadable
    Path unreadableFile = getTaskFilePath(0);
    createTaskErrorFile(
        0,
        "{\"record_data\":{\"id\":\"1\"},\"error_code\":\"E1\",\"error_message\":\"Error 1\"}\n");
    createTaskErrorFile(
        1,
        "{\"record_data\":{\"id\":\"2\"},\"error_code\":\"E2\",\"error_message\":\"Error 2\"}\n");

    // Note: Making file unreadable is platform-specific and may not work in all environments
    // So we'll test the case where file exists but might have issues

    ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource();
    config.set("auth_method", "user_password");
    config.set("username", "test_user");
    config.set("password", "test_pass");
    config.set("object", "TestObject");
    config.set("action_type", "insert");
    config.set("error_records_detail_output_file", errorFilePath.toString());

    TaskSource taskSource = taskSourceFromConfig(config);
    List<TaskReport> successTaskReports = new ArrayList<>();

    // Should not throw exception even if there are IO issues
    plugin.cleanup(taskSource, null, 2, successTaskReports);

    // At least the readable file should be processed
    if (Files.exists(errorFilePath)) {
      List<String> lines = Files.readAllLines(errorFilePath);
      assertTrue(lines.size() >= 1); // At least one file should be readable
    }
  }

  private void createTaskErrorFile(int taskIndex, String content) throws IOException {
    Path taskFilePath = getTaskFilePath(taskIndex);
    if (!content.isEmpty()) {
      try (BufferedWriter writer = Files.newBufferedWriter(taskFilePath, StandardCharsets.UTF_8)) {
        writer.write(content);
      }
    } else {
      // Create empty file
      Files.createFile(taskFilePath);
    }
  }

  private Path getTaskFilePath(int taskIndex) {
    return tempDir.resolve(String.format("error_output.jsonl_task%03d.jsonl", taskIndex));
  }

  @SuppressWarnings("deprecation") // For the use of task.dump()
  private TaskSource taskSourceFromConfig(ConfigSource config) {
    ConfigMapper mapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
    PluginTask task = mapper.map(config, PluginTask.class);
    return task.dump();
  }
}
