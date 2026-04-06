package org.embulk.output.sf_bulk_api;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import com.sforce.soap.partner.sobject.SObject;
import java.util.Calendar;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.type.Types;
import org.junit.Test;

public class TestSForceColumnVisitor {
  private final PageReader pageReader = mock(PageReader.class);

  // --- Basic type tests ---

  @Test
  public void testBooleanColumnNotNull() {
    final SObject record = new SObject();
    final Column column = new Column(0, "test", Types.BOOLEAN);
    final SForceColumnVisitor visitor = new SForceColumnVisitor(record, pageReader, false);

    doReturn(false).when(pageReader).isNull(column);
    doReturn(true).when(pageReader).getBoolean(column);

    visitor.booleanColumn(column);
    assertEquals(true, record.getField("test"));
  }

  @Test
  public void testLongColumnNotNull() {
    final SObject record = new SObject();
    final Column column = new Column(0, "test", Types.LONG);
    final SForceColumnVisitor visitor = new SForceColumnVisitor(record, pageReader, false);

    doReturn(false).when(pageReader).isNull(column);
    doReturn(1L).when(pageReader).getLong(column);

    visitor.longColumn(column);
    assertEquals(1.0, record.getField("test"));
  }

  @Test
  public void testDoubleColumnNotNull() {
    final SObject record = new SObject();
    final Column column = new Column(0, "test", Types.DOUBLE);
    final SForceColumnVisitor visitor = new SForceColumnVisitor(record, pageReader, false);

    doReturn(false).when(pageReader).isNull(column);
    doReturn(1.1).when(pageReader).getDouble(column);

    visitor.doubleColumn(column);
    assertEquals(1.1, record.getField("test"));
  }

  @Test
  public void testStringColumnNotNull() {
    final SObject record = new SObject();
    final Column column = new Column(0, "test", Types.STRING);
    final SForceColumnVisitor visitor = new SForceColumnVisitor(record, pageReader, false);

    doReturn(false).when(pageReader).isNull(column);
    doReturn("hello").when(pageReader).getString(column);

    visitor.stringColumn(column);
    assertEquals("hello", record.getField("test"));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testTimestampColumnNotNull() {
    final SObject record = new SObject();
    final Column column = new Column(0, "test", Types.TIMESTAMP);
    final SForceColumnVisitor visitor = new SForceColumnVisitor(record, pageReader, false);

    doReturn(false).when(pageReader).isNull(column);
    doReturn(org.embulk.spi.time.Timestamp.ofEpochMilli(100)).when(pageReader).getTimestamp(column);

    visitor.timestampColumn(column);
    assertEquals(100, ((Calendar) record.getField("test")).getTimeInMillis());
  }

  // --- Null handling: ignore_nulls=false ---

  @Test
  public void testNullColumnAddsToFieldsToNull() {
    final SObject record = new SObject();
    final Column column = new Column(0, "Name", Types.STRING);
    final SForceColumnVisitor visitor = new SForceColumnVisitor(record, pageReader, false);

    doReturn(true).when(pageReader).isNull(column);

    visitor.stringColumn(column);
    assertArrayEquals(new String[] {"Name"}, visitor.getFieldsToNull());
  }

  @Test
  public void testNullLongColumnAddsToFieldsToNull() {
    final SObject record = new SObject();
    final Column column = new Column(0, "Amount", Types.LONG);
    final SForceColumnVisitor visitor = new SForceColumnVisitor(record, pageReader, false);

    doReturn(true).when(pageReader).isNull(column);

    visitor.longColumn(column);
    assertArrayEquals(new String[] {"Amount"}, visitor.getFieldsToNull());
  }

  // --- Null handling: ignore_nulls=true ---

  @Test
  public void testNullColumnIgnoredWhenIgnoreNullsTrue() {
    final SObject record = new SObject();
    final Column column = new Column(0, "Name", Types.STRING);
    final SForceColumnVisitor visitor = new SForceColumnVisitor(record, pageReader, true);

    doReturn(true).when(pageReader).isNull(column);

    visitor.stringColumn(column);
    assertArrayEquals(new String[0], visitor.getFieldsToNull());
  }

  // --- Multiple columns ---

  @Test
  public void testMultipleColumnsProcessed() {
    final SObject record = new SObject();
    final Column col1 = new Column(0, "FirstName", Types.STRING);
    final Column col2 = new Column(1, "LastName", Types.STRING);
    final SForceColumnVisitor visitor = new SForceColumnVisitor(record, pageReader, false);

    doReturn(false).when(pageReader).isNull(col1);
    doReturn("John").when(pageReader).getString(col1);
    doReturn(false).when(pageReader).isNull(col2);
    doReturn("Doe").when(pageReader).getString(col2);

    visitor.stringColumn(col1);
    visitor.stringColumn(col2);
    assertEquals("John", record.getField("FirstName"));
    assertEquals("Doe", record.getField("LastName"));
  }

  @Test
  public void testMixedNullAndNonNullColumns() {
    final SObject record = new SObject();
    final Column col1 = new Column(0, "Name", Types.STRING);
    final Column col2 = new Column(1, "Email", Types.STRING);
    final SForceColumnVisitor visitor = new SForceColumnVisitor(record, pageReader, false);

    doReturn(false).when(pageReader).isNull(col1);
    doReturn("John").when(pageReader).getString(col1);
    doReturn(true).when(pageReader).isNull(col2);

    visitor.stringColumn(col1);
    visitor.stringColumn(col2);
    assertEquals("John", record.getField("Name"));
    assertArrayEquals(new String[] {"Email"}, visitor.getFieldsToNull());
  }
}
