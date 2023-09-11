package org.embulk.output.sf_bulk_api;

import com.sforce.soap.partner.sobject.SObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

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
      record.addField(column.getName(), Long.toString(pageReader.getLong(column)));
    }
  }

  @Override
  public void doubleColumn(Column column) {
    if (pageReader.isNull(column)) {
      addFieldsToNull(column);
    } else {
      record.addField(column.getName(), Double.toString(pageReader.getDouble(column)));
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

  @Override
  public void timestampColumn(Column column) {
    if (pageReader.isNull(column)) {
      addFieldsToNull(column);
    } else {
      Timestamp timestamp = pageReader.getTimestamp(column);
      DateTime dateTime = new DateTime(timestamp.getEpochSecond() * MILLISECOND, DateTimeZone.UTC);
      record.addField(column.getName(), dateTime.toCalendar(Locale.ENGLISH));
    }
  }

  @Override
  public void jsonColumn(Column column) {
    if (pageReader.isNull(column)) {
      addFieldsToNull(column);
    } else {
      record.addField(column.getName(), pageReader.getString(column));
    }
  }

  private void addFieldsToNull(Column column) {
    if (!ignoreNulls) {
      fieldsToNull.add(column.getName());
    }
  }
}
