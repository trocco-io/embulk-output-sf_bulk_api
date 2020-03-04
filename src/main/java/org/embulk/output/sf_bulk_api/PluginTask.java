package org.embulk.output.sf_bulk_api;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

interface PluginTask extends Task
{
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
    @ConfigDefault("\"null\"")
    String getUpsertKey();
}
