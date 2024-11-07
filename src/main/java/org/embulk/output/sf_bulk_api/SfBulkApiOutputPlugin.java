package org.embulk.output.sf_bulk_api;

import com.sforce.ws.ConnectionException;
import java.util.List;
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

  protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  @Override
  public ConfigDiff transaction(
      ConfigSource config, org.embulk.spi.Schema schema, int taskCount, Control control) {
    final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
    final PluginTask task = configMapper.map(config, PluginTask.class);
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
      List<TaskReport> successTaskReports) {}

  @Override
  public TransactionalPageOutput open(
      TaskSource taskSource, org.embulk.spi.Schema schema, int taskIndex) {
    try {
      final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
      final PluginTask task = taskMapper.map(taskSource, PluginTask.class);
      final ErrorHandler handler = new ErrorHandler(schema);
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

  private static class ExecutionInterruptedException extends RuntimeException {
    public ExecutionInterruptedException(Exception e) {
      super(e);
    }
  }
}
