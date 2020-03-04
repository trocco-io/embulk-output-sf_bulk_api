package org.embulk.output.sf_bulk_api;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;

import com.sforce.soap.partner.sobject.SObject;

public class SForceColumnVisitor implements ColumnVisitor
{
    private final SObject record;
    private final PageReader pageReader;

    public SForceColumnVisitor(SObject record, PageReader pageReader)
    {
        this.record = record;
        this.pageReader = pageReader;
    }

    @Override
    public void booleanColumn(Column column)
    {
        record.addField(column.getName(), pageReader.getBoolean(column));
    }

    @Override
    public void longColumn(Column column)
    {
        record.addField(column.getName(), pageReader.getLong(column));
    }

    @Override
    public void doubleColumn(Column column)
    {
        record.addField(column.getName(), pageReader.getDouble(column));
    }

    @Override
    public void stringColumn(Column column)
    {
        record.addField(column.getName(), pageReader.getString(column));
    }

    @Override
    public void timestampColumn(Column column)
    {
        record.addField(column.getName(), pageReader.getTimestamp(column));
    }

    @Override
    public void jsonColumn(Column column)
    {
        record.addField(column.getName(), pageReader.getJson(column));
    }
}
