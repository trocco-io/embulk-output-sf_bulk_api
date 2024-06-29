package org.embulk.output.sf_bulk_api;

import com.sforce.ws.ConnectorConfig;

public class OauthConnectorConfigCreater implements ConnectorConfigCreater {
  final PluginTask pluginTask;

  OauthConnectorConfigCreater(final PluginTask pluginTask) {
    this.pluginTask = pluginTask;
  }

  public ConnectorConfig createConnectorConfig() {
    ConnectorConfig config = new ConnectorConfig();
    config.setSessionId(pluginTask.getAccessToken().get());
    config.setServiceEndpoint(pluginTask.getServerUrl().get());
    return config;
  }
}
