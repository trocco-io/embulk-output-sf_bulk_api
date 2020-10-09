package org.embulk.output.sf_bulk_api;

import java.util.List;

import com.sforce.ws.ConnectionException;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.exec.ExecutionInterruptedException;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.PageReader;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SfBulkApiOutputPlugin implements OutputPlugin
{
    private final Logger logger = LoggerFactory.getLogger(SfBulkApiOutputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config, org.embulk.spi.Schema schema, int taskCount, Control control)
    {
        final PluginTask task = config.loadConfig(PluginTask.class);
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, org.embulk.spi.Schema schema, int taskCount, Control control)
    {
        throw new UnsupportedOperationException("does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource, org.embulk.spi.Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, org.embulk.spi.Schema schema, int taskIndex)
    {
        try {
            final PluginTask task = taskSource.loadTask(PluginTask.class);
            final ForceClient client = new ForceClient(task);
            PageReader pageReader = new PageReader(schema);
            return new SForceTransactionalPageOutput(client, pageReader, task);
        }
        catch (ConnectionException e) {
            logger.error(e.getMessage(), e);
            throw new ConfigException(e);
        }
        catch (final Exception e) {
            logger.error(e.getMessage(), e);
            throw new ExecutionInterruptedException(e);
        }
    }
}
