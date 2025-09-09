package org.embulk.output.sf_bulk_api;

import com.sforce.soap.partner.sobject.SObject;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;

public class SForceColumnVisitor implements ColumnVisitor {
  private final List<String> fieldsToNull = new ArrayList<>();
  private final SObject record;
  private final PageReader pageReader;
  private final boolean ignoreNulls;
  private static final int MILLISECOND = 1000;

  public SForceColumnVisitor(SObject record, PageReader pageReader, boolean ignoreNulls) {
    this.record = record;
    this.pageReader = pageReader;
    this.ignoreNulls = ignoreNulls;
  }

  public String[] getFieldsToNull() {
    return fieldsToNull.toArray(new String[0]);
  }

  @Override
  public void booleanColumn(Column column) {
    if (pageReader.isNull(column)) {
      addFieldsToNull(column);
    } else {
      record.addField(column.getName(), pageReader.getBoolean(column));
    }
  }

  @Override
  public void longColumn(Column column) {
    if (pageReader.isNull(column)) {
      addFieldsToNull(column);
    } else {
      // The number in SaleForce is treated as a real number. So it is added as double type.
      // https://help.salesforce.com/s/articleView?id=sf.custom_field_types.htm&type=5
      // If it is added as long type, it couldn't upsert values correctly.
      record.addField(column.getName(), (double) pageReader.getLong(column));
    }
  }

  @Override
  public void doubleColumn(Column column) {
    if (pageReader.isNull(column)) {
      addFieldsToNull(column);
    } else {
      record.addField(column.getName(), pageReader.getDouble(column));
    }
  }

  @Override
  public void stringColumn(Column column) {
    if (pageReader.isNull(column)) {
      addFieldsToNull(column);
    } else {
      record.addField(column.getName(), pageReader.getString(column));
    }
  }

  // For the use of org.embulk.spi.time.Timestamp and pageReader.getTimestamp
  @SuppressWarnings("deprecation")
  @Override
  public void timestampColumn(Column column) {
    if (pageReader.isNull(column)) {
      addFieldsToNull(column);
    } else {
      org.embulk.spi.time.Timestamp timestamp = pageReader.getTimestamp(column);
      Calendar calendar = Calendar.getInstance(Locale.ENGLISH);
      calendar.setTimeInMillis(timestamp.getInstant().toEpochMilli());
      record.addField(column.getName(), calendar);
    }
  }

  @SuppressWarnings("deprecation") // For the use of pageReader.getJson
  @Override
  public void jsonColumn(Column column) {
    if (pageReader.isNull(column)) {
      addFieldsToNull(column);
    } else {
      record.addField(column.getName(), pageReader.getJson(column).toJson());
    }
  }

  private void addFieldsToNull(Column column) {
    if (!ignoreNulls) {
      fieldsToNull.add(column.getName());
    }
  }
}
