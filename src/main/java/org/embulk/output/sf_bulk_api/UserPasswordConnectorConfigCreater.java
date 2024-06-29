package org.embulk.output.sf_bulk_api;

import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class UserPasswordConnectorConfigCreater implements ConnectorConfigCreater {
  final PluginTask pluginTask;

  UserPasswordConnectorConfigCreater(final PluginTask pluginTask) {
    this.pluginTask = pluginTask;
  }

  public ConnectorConfig createConnectorConfig() throws ConnectionException {
    ConnectorConfig config = new ConnectorConfig();
    config.setUsername(pluginTask.getUsername().get());
    config.setPassword(pluginTask.getPassword().get() + pluginTask.getSecurityToken().get());
    config.setAuthEndpoint(pluginTask.getAuthEndPoint().get() + pluginTask.getApiVersion() + "/");

    return config;
  }
}
