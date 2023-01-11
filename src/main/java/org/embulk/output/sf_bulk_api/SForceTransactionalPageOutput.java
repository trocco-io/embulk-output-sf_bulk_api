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
    private final ErrorHandler errorHandler;
    private final Logger logger =  LoggerFactory.getLogger(SForceTransactionalPageOutput.class);
    private boolean failed;
    private long failures;

    public SForceTransactionalPageOutput(ForceClient forceClient, PageReader pageReader, PluginTask pluginTask, ErrorHandler errorHandler)
    {
        this.forceClient = forceClient;
        this.pageReader = pageReader;
        this.pluginTask = pluginTask;
        this.errorHandler = errorHandler;
    }

    @Override
    public void add(Page page)
    {
        try {
            List<SObject> records = new ArrayList<>();
            pageReader.setPage(page);
            while (pageReader.nextRecord()) {
                final SObject record = new SObject();
                record.setType(this.pluginTask.getObject());
                pageReader.getSchema().visitColumns(new SForceColumnVisitor(record, pageReader));
                records.add(record);
                if (records.size() >= BATCH_SIZE) {
                    try {
                        failures += forceClient.action(records);
                        failed = failures != 0;
                    } catch (ApiFault e) {
                        // even if some records failed to register, processing continues.
                        failures += errorHandler.handleFault(records, e);
                        failed = true;
                    }
                    records = new ArrayList<>();
                }
            }

            if (CollectionUtils.isNotEmpty(records)) {
                try {
                    failures += forceClient.action(records);
                    failed = failures != 0;
                } catch (ApiFault e) {
                    failures += errorHandler.handleFault(records, e);
                    failed = true;
                }
            }
        }
        catch (AbortException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
        catch (Exception e) {
            failed = true;
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
        final TaskReport taskReport = Exec.newTaskReport();
        taskReport.set("failed", failed);
        taskReport.set("failures", failures);
        return taskReport;
    }
}
