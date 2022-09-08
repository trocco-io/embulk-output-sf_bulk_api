package org.embulk.output.sf_bulk_api;

import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public interface ConnectorConfigCreater
{
    ConnectorConfig createConnectorConfig() throws ConnectionException;
}
