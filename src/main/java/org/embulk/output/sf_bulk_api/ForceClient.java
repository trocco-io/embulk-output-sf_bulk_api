package org.embulk.output.sf_bulk_api;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.sforce.async.AsyncApiException;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.Connector;
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
    private final ActionType actionType;
    private final String upsertKey;

    public ForceClient(final PluginTask pluginTask) throws ConnectionException, AsyncApiException
    {
        final ConnectorConfig connectorConfig = createConnectorConfig(pluginTask);
        this.partnerConnection = Connector.newConnection(connectorConfig);
        this.actionType = ActionType.convertActionType(pluginTask.getActionType());
        this.upsertKey = pluginTask.getUpsertKey();
    }

    public void action(final List<SObject> sObjects) throws ConnectionException
    {
        logger.info("sObjects size:" + Integer.toString(sObjects.size()));
        switch (this.actionType) {
        case INSERT:
            insert(sObjects);
            break;
        case UPSERT:
            upsert(this.upsertKey, sObjects);
            break;
        case UPDATE:
            update(sObjects);
            break;
        default:
            throw new RuntimeException();
        }
    }

    private ConnectorConfig createConnectorConfig(final PluginTask pluginTask) throws ConnectionException
    {
        final ConnectorConfig config = new ConnectorConfig();
        config.setUsername(pluginTask.getUsername());
        config.setPassword(pluginTask.getPassword() + pluginTask.getSecurityToken());
        config.setAuthEndpoint(pluginTask.getAuthEndPoint() + pluginTask.getApiVersion() + "/");
        return config;
    }

    private void insert(final List<SObject> sObjects) throws ConnectionException
    {
        try {
            final SaveResult[] saveResultArray = partnerConnection.create(sObjects.toArray(new SObject[sObjects.size()]));
            loggingSaveErrorMessage(saveResultArray);
        }
        catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void upsert(final String key, final List<SObject> sObjects) throws ConnectionException
    {
        try {
            final UpsertResult[] upsertResultArray = partnerConnection.upsert(key, sObjects.toArray(new SObject[sObjects.size()]));
            final List<UpsertResult> upsertResults = Arrays.asList(upsertResultArray);
            upsertResults.forEach(result -> {
                if (!result.isSuccess()) {
                    final List<String> errors = Arrays.asList(result.getErrors())
                                                .stream().map(e -> e.getStatusCode() + ":" + e.getMessage())
                                                .collect(Collectors.toList());
                    logger.info(String.join(",", errors));
                }
            });
        }
        catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void update(final List<SObject> sObjects) throws ConnectionException
    {
        try {
            final SaveResult[] saveResultArray = partnerConnection.update(sObjects.toArray(new SObject[sObjects.size()]));
            loggingSaveErrorMessage(saveResultArray);
        }
        catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void loggingSaveErrorMessage(final SaveResult[] saveResultArray)
    {
        final List<SaveResult> saveResults = Arrays.asList(saveResultArray);
        saveResults.forEach(result -> {
            if (!result.isSuccess()) {
                final List<String> errors = Arrays.asList(result.getErrors())
                                            .stream().map(e -> e.getStatusCode() + ":" + e.getMessage())
                                            .collect(Collectors.toList());
                logger.info(String.join(",", errors));
            }
            logger.info(result.getId());
        });
    }

    private enum ActionType
    {
        INSERT, UPSERT, UPDATE;

        public static ActionType convertActionType(final String key)
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
