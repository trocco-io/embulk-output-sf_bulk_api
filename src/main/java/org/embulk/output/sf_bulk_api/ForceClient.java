package org.embulk.output.sf_bulk_api;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.sforce.async.AsyncApiException;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ForceClient
 */
public class ForceClient
{
    private final PartnerConnection partnerConnection;
    private final Logger logger =  LoggerFactory.getLogger(ForceClient.class);
    private ActionType actionType;
    private String upsertKey;

    public ForceClient(final PluginTask pluginTask) throws ConnectionException, AsyncApiException
    {
        final ConnectorConfig connectorConfig = createConnectorConfig(pluginTask);
        this.partnerConnection = new PartnerConnection(connectorConfig);
        this.actionType = ActionType.convertActionType(pluginTask.getActionType());
        this.upsertKey = pluginTask.getUpsertKey();
    }

    public void action(final List<SObject> sObjects) throws ConnectionException
    {
        switch (this.actionType) {
        case INSERT:
            insert(sObjects);
        case UPSERT:
            upsert(this.upsertKey, sObjects);
        case UPDATE:
            update(sObjects);
        }
    }

    public void logout() throws ConnectionException
    {
        this.partnerConnection.logout();
    }

    private ConnectorConfig createConnectorConfig(final PluginTask pluginTask) throws ConnectionException
    {
        final ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(pluginTask.getUsername());
        partnerConfig.setPassword(pluginTask.getPassword() + pluginTask.getSecurityToken());
        partnerConfig.setAuthEndpoint(pluginTask.getAuthEndPoint() + pluginTask.getApiVersion());
        new PartnerConnection(partnerConfig);

        final ConnectorConfig config = new ConnectorConfig();
        config.setSessionId(partnerConfig.getSessionId());
        final String soapEndpoint = partnerConfig.getServiceEndpoint();
        final String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/")) + "async/" + pluginTask.getApiVersion();
        config.setRestEndpoint(restEndpoint);
        config.setCompression(true);
        config.setTraceMessage(false);
        return config;
    }

    private void insert(final List<SObject> sObjects) throws ConnectionException
    {
        final SaveResult[] saveResultArray = partnerConnection.create(sObjects.toArray(new SObject[sObjects.size()]));
        loggingSaveErrorMessage(saveResultArray);
    }

    private void upsert(String key, final List<SObject> sObjects) throws ConnectionException
    {
        final UpsertResult[] upsertResultArray = partnerConnection.upsert(key, sObjects.toArray(new SObject[sObjects.size()]));
        final List<UpsertResult> upsertResults = Arrays.asList(upsertResultArray);
        upsertResults.forEach(result -> {
            if (!result.isSuccess()) {
                List<String> errors = Arrays.asList(result.getErrors())
                                            .stream().map(e -> e.getStatusCode() + ":" + e.getMessage())
                                            .collect(Collectors.toList());
                logger.info(String.join(",", errors));
            }
        });
    }

    private void update(final List<SObject> sObjects) throws ConnectionException
    {
        final SaveResult[] saveResultArray = partnerConnection.update(sObjects.toArray(new SObject[sObjects.size()]));
        loggingSaveErrorMessage(saveResultArray);
    }

    private void loggingSaveErrorMessage(final SaveResult[] saveResultArray)
    {
        final List<SaveResult> saveResults = Arrays.asList(saveResultArray);
        saveResults.forEach(result -> {
            if (!result.isSuccess()) {
                List<String> errors = Arrays.asList(result.getErrors())
                                            .stream().map(e -> e.getStatusCode() + ":" + e.getMessage())
                                            .collect(Collectors.toList());
                logger.info(String.join(",", errors));
            }
        });
    }

    private enum ActionType
    {
        INSERT, UPSERT, UPDATE;

        public static ActionType convertActionType(String key)
        {
            switch (key) {
            case "insert":
                return INSERT;
            case "upsert":
                return UPSERT;
            case "update":
                return UPDATE;
            default:
                return null;
            }
        }
    }
}
