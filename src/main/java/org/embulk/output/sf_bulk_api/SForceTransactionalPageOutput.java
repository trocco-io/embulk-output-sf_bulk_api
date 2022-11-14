package org.embulk.output.sf_bulk_api;

import java.util.ArrayList;
import java.util.List;

import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;

import org.apache.commons.collections.CollectionUtils;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SForceTransactionalPageOutput implements TransactionalPageOutput
{
    private static final Long BATCH_SIZE = 200L;

    private final ForceClient forceClient;
    private final PageReader pageReader;
    private final PluginTask pluginTask;

    private final Logger logger =  LoggerFactory.getLogger(SForceTransactionalPageOutput.class);

    public SForceTransactionalPageOutput(ForceClient forceClient, PageReader pageReader, PluginTask pluginTask)
    {
        this.forceClient = forceClient;
        this.pageReader = pageReader;
        this.pluginTask = pluginTask;
    }

    @Override
    public void add(Page page)
    {
        try {
            ArrayList records = new ArrayList<>();
            pageReader.setPage(page);
            while (pageReader.nextRecord()) {
                final SObject record = new SObject();
                record.setType(this.pluginTask.getObject());
                pageReader.getSchema().visitColumns(new SForceColumnVisitor(record, pageReader));
                records.add(record);
                if (records.size() >= BATCH_SIZE) {
                    try {
                        forceClient.action(records);
                    } catch (ApiFault e) {
                        // even if some records failed to register, processing continues.
                        logger.error(e.getExceptionCode().toString() + ":" + e.getExceptionMessage(), e);
                    }
                    records = new ArrayList<>();
                }
            }

            if (CollectionUtils.isNotEmpty(records)) {
                forceClient.action(records);
            }
        }
        catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void finish()
    {
    }

    @Override
    public void close()
    {
        try {
            forceClient.logout();
        }
        catch (ConnectionException e) {
        }
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
