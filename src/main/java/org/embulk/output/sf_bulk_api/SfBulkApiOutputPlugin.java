package org.embulk.output.sf_bulk_api;

import com.sforce.ws.ConnectionException;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.DataException;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.PageReader;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.TaskMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SfBulkApiOutputPlugin implements OutputPlugin {
  private final Logger logger = LoggerFactory.getLogger(SfBulkApiOutputPlugin.class);

  public static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  @SuppressWarnings("deprecation") // For the use of task.dump()
  @Override
  public ConfigDiff transaction(
      ConfigSource config, org.embulk.spi.Schema schema, int taskCount, Control control) {
    final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
    final PluginTask task = configMapper.map(config, PluginTask.class);
    int batchSize = task.getBatchSize();
    if (batchSize < 1 || batchSize > 200) {
      throw new ConfigException("batch_size must be between 1 and 200");
    }
    if (task.getUpdateKey().isPresent() && !"update".equals(task.getActionType())) {
      throw new ConfigException("update_key can only be used with action_type: update");
    }
    if (task.getUpdateKey().isPresent()) {
      String updateKey = task.getUpdateKey().get();
      boolean exists =
          schema.getColumns().stream().anyMatch(column -> column.getName().equals(updateKey));
      if (!exists) {
        throw new ConfigException(
            String.format("update_key '%s' does not exist in input schema", updateKey));
      }
    }
    Set<String> seenReferenceFields = new HashSet<>();
    for (AssociationConfig assoc : task.getAssociations()) {
      boolean exists =
          schema.getColumns().stream()
              .anyMatch(col -> col.getName().equals(assoc.getSourceColumn()));
      if (!exists) {
        throw new ConfigException(
            String.format(
                "association source_column '%s' does not exist in input schema",
                assoc.getSourceColumn()));
      }
      if (!seenReferenceFields.add(assoc.getReferenceField())) {
        throw new ConfigException(
            String.format("duplicate association reference_field '%s'", assoc.getReferenceField()));
      }
    }
    final List<TaskReport> taskReports = control.run(task.dump());
    final long failures =
        taskReports.stream().mapToLong(taskReport -> taskReport.get(long.class, "failures")).sum();
    final boolean failed =
        taskReports.stream().anyMatch(taskReport -> taskReport.get(boolean.class, "failed"));
    if (task.getThrowIfFailed() && failed) {
      throw new DataException(String.format("There are %,d failures", failures));
    }
    return CONFIG_MAPPER_FACTORY.newConfigDiff();
  }

  @Override
  public ConfigDiff resume(
      TaskSource taskSource, org.embulk.spi.Schema schema, int taskCount, Control control) {
    throw new UnsupportedOperationException("does not support resuming");
  }

  @Override
  public void cleanup(
      TaskSource taskSource,
      org.embulk.spi.Schema schema,
      int taskCount,
      List<TaskReport> successTaskReports) {
    final PluginTask task =
        CONFIG_MAPPER_FACTORY.createTaskMapper().map(taskSource, PluginTask.class);

    // Concatenate error files if error output is configured
    task.getErrorRecordsDetailOutputFile().ifPresent(this::concatenateErrorFiles);
  }

  // For the use of org.embulk.spi.PageReaderのPageReader(org.embulk.spi.Schema).
  @SuppressWarnings("deprecation")
  @Override
  public TransactionalPageOutput open(
      TaskSource taskSource, org.embulk.spi.Schema schema, int taskIndex) {
    try {
      final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
      final PluginTask task = taskMapper.map(taskSource, PluginTask.class);
      final ErrorHandler handler =
          task.getErrorRecordsDetailOutputFile()
              .map(outputPath -> new ErrorHandler(schema, outputPath, taskIndex))
              .orElse(new ErrorHandler(schema));
      final ForceClient client = new ForceClient(task, handler);
      PageReader pageReader = new PageReader(schema);
      return new SForceTransactionalPageOutput(client, pageReader, task, handler);
    } catch (ConnectionException e) {
      logger.error(e.getMessage(), e);
      throw new ConfigException(e);
    } catch (final Exception e) {
      logger.error(e.getMessage(), e);
      throw new ExecutionInterruptedException(e);
    }
  }

  private void concatenateErrorFiles(String outputFile) {
    Path outputPath = Paths.get(outputFile);
    Path directory = outputPath.getParent();
    String baseFileName = outputPath.getFileName().toString();

    try {
      List<Path> taskFiles =
          Files.list(directory)
              .filter(path -> path.getFileName().toString().startsWith(baseFileName + "_task"))
              .sorted()
              .collect(java.util.stream.Collectors.toList());

      // If no task files exist, don't create output file
      if (taskFiles.isEmpty()) {
        return;
      }

      try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
        boolean hasContent = false;
        for (Path taskFile : taskFiles) {
          try {
            List<String> lines = Files.readAllLines(taskFile);
            if (!lines.isEmpty()) {
              hasContent = true;
              for (String line : lines) {
                writer.write(line);
                writer.newLine();
              }
            }
            Files.deleteIfExists(taskFile);
          } catch (IOException e) {
            logger.error("Failed to process task file: " + taskFile, e);
          }
        }

        // If no content was written, delete the empty output file
        if (!hasContent) {
          writer.close();
          Files.deleteIfExists(outputPath);
        }
      }
    } catch (IOException e) {
      logger.error("Failed to concatenate error files", e);
    }
  }

  private static class ExecutionInterruptedException extends RuntimeException {
    public ExecutionInterruptedException(Exception e) {
      super(e);
    }
  }
}
