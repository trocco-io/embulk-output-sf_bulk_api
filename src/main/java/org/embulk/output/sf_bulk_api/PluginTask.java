package org.embulk.output.sf_bulk_api;

import java.util.List;
import java.util.Optional;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

public interface PluginTask extends Task {
  // The SOAP login() call used by auth_method: user_password is not available in API versions
  // 65.0 and later, so this is both the default api_version and the maximum version accepted
  // with user_password (validated in SfBulkApiOutputPlugin).
  String MAX_USER_PASSWORD_API_VERSION = "64.0";

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
  @ConfigDefault("\"" + MAX_USER_PASSWORD_API_VERSION + "\"")
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

  @Config("batch_size")
  @ConfigDefault("200")
  int getBatchSize();

  @Config("update_key")
  @ConfigDefault("null")
  Optional<String> getUpdateKey();

  @Config("delete_key")
  @ConfigDefault("\"Id\"")
  String getDeleteKey();

  @Config("error_records_detail_output_file")
  @ConfigDefault("null")
  Optional<String> getErrorRecordsDetailOutputFile();

  @Config("associations")
  @ConfigDefault("[]")
  List<AssociationConfig> getAssociations();
}
