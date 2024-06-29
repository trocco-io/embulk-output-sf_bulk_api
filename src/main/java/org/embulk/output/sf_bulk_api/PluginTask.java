package org.embulk.output.sf_bulk_api;

import java.util.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

public interface PluginTask extends Task {
  @Config("auth_method")
  @ConfigDefault("\"user_password\"")
  AuthMethod getAuthMethod();

  @Config("server_url")
  @ConfigDefault("null")
  Optional<String> getServerUrl();

  @Config("access_token")
  @ConfigDefault("null")
  Optional<String> getAccessToken();

  @Config("username")
  @ConfigDefault("null")
  Optional<String> getUsername();

  @Config("password")
  @ConfigDefault("null")
  Optional<String> getPassword();

  @Config("api_version")
  @ConfigDefault("\"46.0\"")
  String getApiVersion();

  @Config("security_token")
  @ConfigDefault("null")
  Optional<String> getSecurityToken();

  @Config("auth_end_point")
  @ConfigDefault("\"https://login.salesforce.com/services/Soap/u/\"")
  Optional<String> getAuthEndPoint();

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
