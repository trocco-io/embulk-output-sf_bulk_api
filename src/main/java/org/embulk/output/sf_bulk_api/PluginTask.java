package org.embulk.output.sf_bulk_api;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

public interface PluginTask extends Task {
  @Config("username")
  String getUsername();

  @Config("password")
  String getPassword();

  @Config("api_version")
  @ConfigDefault("\"46.0\"")
  String getApiVersion();

  @Config("security_token")
  String getSecurityToken();

  @Config("auth_end_point")
  @ConfigDefault("\"https://login.salesforce.com/services/Soap/u/\"")
  String getAuthEndPoint();

  @Config("object")
  String getObject();

  @Config("action_type")
  String getActionType();

  @Config("upsert_key")
  @ConfigDefault("\"key\"")
  String getUpsertKey();

  @Config("ignore_nulls")
  @ConfigDefault("\"true\"")
  boolean getIgnoreNulls();

  @Config("throw_if_failed")
  @ConfigDefault("\"true\"")
  boolean getThrowIfFailed();
}
