package org.embulk.output.sf_bulk_api;

import java.util.ArrayList;
import java.util.List;

import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;

import org.apache.commons.collections.CollectionUtils;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.exec.ExecutionInterruptedException;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SForceTransactionalPageOutput implements TransactionalPageOutput
{
    private static final Long BATCH_SIZE = 1000L;

    private final ForceClient forceClient;
    private final PageReader pageReader;
    private final PluginTask pluginTask;
    private List<SObject> records;

    private final Logger logger =  LoggerFactory.getLogger(SForceTransactionalPageOutput.class);

    public SForceTransactionalPageOutput(ForceClient forceClient, PageReader pageReader, PluginTask pluginTask)
    {
        this.forceClient = forceClient;
        this.pageReader = pageReader;
        this.pluginTask = pluginTask;
        this.records = new ArrayList<>();
    }

    @Override
    public void add(Page page)
    {
        try {
            pageReader.setPage(page);
            while (pageReader.nextRecord()) {
                final SObject record = new SObject();
                record.setType(this.pluginTask.getObject());
                pageReader.getSchema().visitColumns(new SForceColumnVisitor(record, pageReader));
                this.records.add(record);
                if (this.records.size() >= BATCH_SIZE) {
                    forceClient.action(records);
                }
            }

            if (CollectionUtils.isNotEmpty(records)) {
                forceClient.action(records);
            }
        }
        catch (ConnectionException e) {
            logger.error(e.getMessage(), e);
            throw new ConfigException(e);
        }
        catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new ExecutionInterruptedException(e);
        }
    }

    @Override
    public void finish()
    {
    }

    @Override
    public void close()
    {
    }

    @Override
    public void abort()
    {
    }

    @Override
    public TaskReport commit()
    {
        return Exec.newTaskReport();
    }
}
