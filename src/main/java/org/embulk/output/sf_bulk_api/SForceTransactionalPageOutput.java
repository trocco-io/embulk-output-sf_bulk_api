package org.embulk.output.sf_bulk_api;

import static org.embulk.output.sf_bulk_api.SfBulkApiOutputPlugin.CONFIG_MAPPER_FACTORY;

import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.soap.partner.sobject.SObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.spi.Column;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SForceTransactionalPageOutput implements TransactionalPageOutput {
  private final int batchSize;

  private final ForceClient forceClient;
  private final PageReader pageReader;
  private final PluginTask pluginTask;
  private final ErrorHandler errorHandler;
  private final Map<AssociationConfig, Column> associationColumns;
  private final Set<String> associationSourceColumns;

  private final Logger logger = LoggerFactory.getLogger(SForceTransactionalPageOutput.class);
  private boolean failed;
  private long failures;

  public SForceTransactionalPageOutput(
      ForceClient forceClient,
      PageReader pageReader,
      PluginTask pluginTask,
      ErrorHandler errorHandler) {
    this.forceClient = forceClient;
    this.pageReader = pageReader;
    this.pluginTask = pluginTask;
    this.errorHandler = errorHandler;
    this.batchSize = pluginTask.getBatchSize();
    Schema schema = pageReader.getSchema();
    List<AssociationConfig> associations = pluginTask.getAssociations();
    this.associationColumns = new LinkedHashMap<>();
    for (AssociationConfig assoc : associations) {
      associationColumns.put(assoc, findColumn(schema, assoc.getSourceColumn()));
    }
    this.associationSourceColumns =
        associations.stream().map(AssociationConfig::getSourceColumn).collect(Collectors.toSet());
  }

  @Override
  public void add(Page page) {
    try {
      List<SObject> records = new ArrayList<>();
      pageReader.setPage(page);
      while (pageReader.nextRecord()) {
        final SObject record = new SObject();
        record.setType(this.pluginTask.getObject());
        SForceColumnVisitor visitor =
            new SForceColumnVisitor(
                record, pageReader, pluginTask.getIgnoreNulls(), associationSourceColumns);
        pageReader.getSchema().visitColumns(visitor);

        List<String> fieldsToNull = new ArrayList<>(Arrays.asList(visitor.getFieldsToNull()));
        for (Map.Entry<AssociationConfig, Column> entry : associationColumns.entrySet()) {
          AssociationConfig assoc = entry.getKey();
          Column sourceCol = entry.getValue();
          if (pageReader.isNull(sourceCol)) {
            if (!pluginTask.getIgnoreNulls()) {
              fieldsToNull.add(assoc.getReferenceField());
            }
          } else {
            String value = readColumnAsString(pageReader, sourceCol);
            String relationshipName =
                AssociationConfig.deriveRelationshipName(assoc.getReferenceField());
            SObject refObject = new SObject();
            refObject.setType(assoc.getReferencedObject());
            refObject.setField(assoc.getUniqueKey(), value);
            record.setField(relationshipName, refObject);
          }
        }
        record.setFieldsToNull(fieldsToNull.toArray(new String[0]));
        records.add(record);
        if (records.size() >= batchSize) {
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
    } catch (AbortException e) {
      logger.error(e.getMessage(), e);
      throw e;
    } catch (Exception e) {
      failed = true;
      logger.error(e.getMessage(), e);
    }
  }

  @Override
  public void finish() {}

  @Override
  public void close() {
    // Note: logout() is intentionally not called here.
    // Salesforce SOAP sessions are shared across concurrent connections using the same credentials.
    // Calling logout() destroys the session for ALL holders, causing INVALID_SESSION_ID errors
    // in any other running job that shares the session.
    // Sessions expire automatically after the configured inactivity timeout (default 2 hours).

    // Close error file logger
    if (errorHandler != null) {
      errorHandler.close();
    }
  }

  @Override
  public void abort() {}

  private Column findColumn(Schema schema, String columnName) {
    return schema.getColumns().stream()
        .filter(col -> col.getName().equals(columnName))
        .findFirst()
        .orElseThrow(() -> new ConfigException("Column not found: " + columnName));
  }

  @SuppressWarnings("deprecation")
  private String readColumnAsString(PageReader reader, Column column) {
    switch (column.getType().getName()) {
      case "string":
        return reader.getString(column);
      case "long":
        return String.valueOf(reader.getLong(column));
      case "double":
        return String.valueOf(reader.getDouble(column));
      case "boolean":
        return String.valueOf(reader.getBoolean(column));
      case "timestamp":
        return reader.getTimestamp(column).getInstant().toString();
      case "json":
        return reader.getJson(column).toJson();
      default:
        return reader.getString(column);
    }
  }

  @Override
  public TaskReport commit() {
    final TaskReport taskReport = CONFIG_MAPPER_FACTORY.newTaskReport();
    taskReport.set("failed", failed);
    taskReport.set("failures", failures);
    return taskReport;
  }
}
