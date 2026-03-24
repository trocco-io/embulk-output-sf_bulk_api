package org.embulk.output.sf_bulk_api;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

import com.sforce.soap.partner.sobject.SObject;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.type.Types;
import org.junit.Test;

public class TestSForceColumnVisitor {
  private final PageReader pageReader = mock(PageReader.class);

  // --- Existing basic type tests ---

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

  // --- skipColumns: string ---

  @Test
  public void testSkipColumnString() {
    final SObject record = new SObject();
    final Column column = new Column(0, "account_code", Types.STRING);
    Set<String> skipColumns = new HashSet<>(Collections.singletonList("account_code"));
    final SForceColumnVisitor visitor =
        new SForceColumnVisitor(record, pageReader, false, skipColumns);

    doReturn(false).when(pageReader).isNull(column);
    doReturn("COMP-001").when(pageReader).getString(column);

    visitor.stringColumn(column);
    assertNull(record.getField("account_code"));
  }

  // --- skipColumns: all other types ---

  @Test
  public void testSkipColumnBoolean() {
    final SObject record = new SObject();
    final Column column = new Column(0, "skip_bool", Types.BOOLEAN);
    Set<String> skipColumns = new HashSet<>(Collections.singletonList("skip_bool"));
    final SForceColumnVisitor visitor =
        new SForceColumnVisitor(record, pageReader, false, skipColumns);

    doReturn(false).when(pageReader).isNull(column);
    doReturn(true).when(pageReader).getBoolean(column);

    visitor.booleanColumn(column);
    assertNull(record.getField("skip_bool"));
  }

  @Test
  public void testSkipColumnLong() {
    final SObject record = new SObject();
    final Column column = new Column(0, "skip_long", Types.LONG);
    Set<String> skipColumns = new HashSet<>(Collections.singletonList("skip_long"));
    final SForceColumnVisitor visitor =
        new SForceColumnVisitor(record, pageReader, false, skipColumns);

    doReturn(false).when(pageReader).isNull(column);
    doReturn(42L).when(pageReader).getLong(column);

    visitor.longColumn(column);
    assertNull(record.getField("skip_long"));
  }

  @Test
  public void testSkipColumnDouble() {
    final SObject record = new SObject();
    final Column column = new Column(0, "skip_double", Types.DOUBLE);
    Set<String> skipColumns = new HashSet<>(Collections.singletonList("skip_double"));
    final SForceColumnVisitor visitor =
        new SForceColumnVisitor(record, pageReader, false, skipColumns);

    doReturn(false).when(pageReader).isNull(column);
    doReturn(3.14).when(pageReader).getDouble(column);

    visitor.doubleColumn(column);
    assertNull(record.getField("skip_double"));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSkipColumnTimestamp() {
    final SObject record = new SObject();
    final Column column = new Column(0, "skip_ts", Types.TIMESTAMP);
    Set<String> skipColumns = new HashSet<>(Collections.singletonList("skip_ts"));
    final SForceColumnVisitor visitor =
        new SForceColumnVisitor(record, pageReader, false, skipColumns);

    doReturn(false).when(pageReader).isNull(column);
    doReturn(org.embulk.spi.time.Timestamp.ofEpochMilli(100)).when(pageReader).getTimestamp(column);

    visitor.timestampColumn(column);
    assertNull(record.getField("skip_ts"));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSkipColumnJson() {
    final SObject record = new SObject();
    final Column column = new Column(0, "skip_json", Types.JSON);
    Set<String> skipColumns = new HashSet<>(Collections.singletonList("skip_json"));
    final SForceColumnVisitor visitor =
        new SForceColumnVisitor(record, pageReader, false, skipColumns);

    doReturn(false).when(pageReader).isNull(column);

    visitor.jsonColumn(column);
    assertNull(record.getField("skip_json"));
  }

  // --- skipColumns: null handling ---

  @Test
  public void testSkipColumnNullNotAddedToFieldsToNull() {
    final SObject record = new SObject();
    final Column column = new Column(0, "account_code", Types.STRING);
    Set<String> skipColumns = new HashSet<>(Collections.singletonList("account_code"));
    final SForceColumnVisitor visitor =
        new SForceColumnVisitor(record, pageReader, false, skipColumns);

    doReturn(true).when(pageReader).isNull(column);

    visitor.stringColumn(column);
    assertArrayEquals(new String[0], visitor.getFieldsToNull());
  }

  @Test
  public void testSkipColumnNullLongNotAddedToFieldsToNull() {
    final SObject record = new SObject();
    final Column column = new Column(0, "skip_long", Types.LONG);
    Set<String> skipColumns = new HashSet<>(Collections.singletonList("skip_long"));
    final SForceColumnVisitor visitor =
        new SForceColumnVisitor(record, pageReader, false, skipColumns);

    doReturn(true).when(pageReader).isNull(column);

    visitor.longColumn(column);
    assertArrayEquals(new String[0], visitor.getFieldsToNull());
  }

  // --- skipColumns: mixed with non-skip columns ---

  @Test
  public void testNonSkipColumnStillProcessed() {
    final SObject record = new SObject();
    final Column normalCol = new Column(0, "FirstName", Types.STRING);
    final Column skipCol = new Column(1, "account_code", Types.STRING);
    Set<String> skipColumns = new HashSet<>(Collections.singletonList("account_code"));
    final SForceColumnVisitor visitor =
        new SForceColumnVisitor(record, pageReader, false, skipColumns);

    doReturn(false).when(pageReader).isNull(normalCol);
    doReturn("John").when(pageReader).getString(normalCol);
    doReturn(false).when(pageReader).isNull(skipCol);
    doReturn("COMP-001").when(pageReader).getString(skipCol);

    visitor.stringColumn(normalCol);
    visitor.stringColumn(skipCol);
    assertEquals("John", record.getField("FirstName"));
    assertNull(record.getField("account_code"));
  }

  @Test
  public void testMultipleSkipColumns() {
    final SObject record = new SObject();
    final Column col1 = new Column(0, "Name", Types.STRING);
    final Column col2 = new Column(1, "code1", Types.STRING);
    final Column col3 = new Column(2, "code2", Types.STRING);
    Set<String> skipColumns = new HashSet<>(Arrays.asList("code1", "code2"));
    final SForceColumnVisitor visitor =
        new SForceColumnVisitor(record, pageReader, false, skipColumns);

    doReturn(false).when(pageReader).isNull(col1);
    doReturn("John").when(pageReader).getString(col1);
    doReturn(false).when(pageReader).isNull(col2);
    doReturn("A").when(pageReader).getString(col2);
    doReturn(false).when(pageReader).isNull(col3);
    doReturn("B").when(pageReader).getString(col3);

    visitor.stringColumn(col1);
    visitor.stringColumn(col2);
    visitor.stringColumn(col3);
    assertEquals("John", record.getField("Name"));
    assertNull(record.getField("code1"));
    assertNull(record.getField("code2"));
  }

  // --- backward compat: empty skipColumns behaves as before ---

  @Test
  public void testEmptySkipColumnsBackwardCompat() {
    final SObject record = new SObject();
    final Column column = new Column(0, "Name", Types.STRING);
    final SForceColumnVisitor visitor = new SForceColumnVisitor(record, pageReader, false);

    doReturn(false).when(pageReader).isNull(column);
    doReturn("John").when(pageReader).getString(column);

    visitor.stringColumn(column);
    assertEquals("John", record.getField("Name"));
  }

  @Test
  public void testEmptySkipColumnsNullAddsToFieldsToNull() {
    final SObject record = new SObject();
    final Column column = new Column(0, "Name", Types.STRING);
    final SForceColumnVisitor visitor = new SForceColumnVisitor(record, pageReader, false);

    doReturn(true).when(pageReader).isNull(column);

    visitor.stringColumn(column);
    assertArrayEquals(new String[] {"Name"}, visitor.getFieldsToNull());
  }
}
