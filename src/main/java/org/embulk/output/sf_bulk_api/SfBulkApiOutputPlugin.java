package org.embulk.output.sf_bulk_api;

import com.sforce.ws.ConnectionException;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.exec.ExecutionInterruptedException;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.PageReader;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SfBulkApiOutputPlugin implements OutputPlugin {
  private final Logger logger = LoggerFactory.getLogger(SfBulkApiOutputPlugin.class);

  @Override
  public ConfigDiff transaction(
      ConfigSource config, org.embulk.spi.Schema schema, int taskCount, Control control) {
    final PluginTask task = config.loadConfig(PluginTask.class);
    final List<TaskReport> taskReports = control.run(task.dump());
    final long failures =
        taskReports.stream().mapToLong(taskReport -> taskReport.get(long.class, "failures")).sum();
    final boolean failed =
        taskReports.stream().anyMatch(taskReport -> taskReport.get(boolean.class, "failed"));
    if (task.getThrowIfFailed() && failed) {
      throw new DataException(String.format("There are %,d failures", failures));
    }
    return Exec.newConfigDiff();
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
    final PluginTask task = taskSource.loadTask(PluginTask.class);

    // Concatenate error files if error output is configured
    task.getErrorRecordsDetailOutputFile().ifPresent(this::concatenateErrorFiles);
  }

  @Override
  public TransactionalPageOutput open(
      TaskSource taskSource, org.embulk.spi.Schema schema, int taskIndex) {
    try {
      final PluginTask task = taskSource.loadTask(PluginTask.class);

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
}
