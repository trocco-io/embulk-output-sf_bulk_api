package org.embulk.output.sf_bulk_api;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import com.sforce.soap.partner.sobject.SObject;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.type.Types;
import org.junit.Test;

public class TestSForceColumnVisitor {
  private final PageReader pageReader = mock(PageReader.class);

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
}
