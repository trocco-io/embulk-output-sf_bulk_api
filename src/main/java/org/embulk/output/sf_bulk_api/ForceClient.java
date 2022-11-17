package org.embulk.output.sf_bulk_api;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    private final Map<AuthMethod, ConnectorConfigCreater> connectorConfigCreaters = new HashMap<>();

    public ForceClient(final PluginTask pluginTask) throws ConnectionException
    {
        setConnectorConfigCreaters(pluginTask);
        ConnectorConfigCreater connectorConfigCreater = connectorConfigCreaters.get(pluginTask.getAuthMethod());
        ConnectorConfig connectorConfig = connectorConfigCreater.createConnectorConfig();
        this.partnerConnection = Connector.newConnection(connectorConfig);
        this.actionType = ActionType.convertActionType(pluginTask.getActionType());
        this.upsertKey = pluginTask.getUpsertKey();
    }

    public void action(final List<SObject> sObjects) throws ConnectionException
    {
        logger.info("sObjects size:" + sObjects.size());
        switch (this.actionType) {
        case INSERT:
            insert(sObjects);
            return;
        case UPSERT:
            upsert(this.upsertKey, sObjects);
            return;
        case UPDATE:
            update(sObjects);
            return;
        default:
        }
    }

    public void logout() throws ConnectionException
    {
        this.partnerConnection.logout();
    }

    private void setConnectorConfigCreaters(PluginTask pluginTask)
    {
        connectorConfigCreaters.put(AuthMethod.oauth, new OauthConnectorConfigCreater(pluginTask));
        connectorConfigCreaters.put(AuthMethod.user_password, new UserPasswordConnectorConfigCreater(pluginTask));
    }

    private void insert(final List<SObject> sObjects) throws ConnectionException {
        final SaveResult[] saveResultArray = partnerConnection.create(sObjects.toArray(new SObject[sObjects.size()]));
        loggingSaveErrorMessage(saveResultArray);
    }

    private void upsert(final String key, final List<SObject> sObjects) throws ConnectionException {
        final UpsertResult[] upsertResultArray = partnerConnection.upsert(key, sObjects.toArray(new SObject[sObjects.size()]));
        final List<UpsertResult> upsertResults = Arrays.asList(upsertResultArray);
        upsertResults.forEach(result -> {
            if (!result.isSuccess()) {
                final List<String> errors = Arrays.asList(result.getErrors())
                                            .stream().map(e -> e.getStatusCode() + ":" + e.getMessage())
                                            .collect(Collectors.toList());
                logger.warn(String.join(",", errors));
            }
        });
    }

    private void update(final List<SObject> sObjects) throws ConnectionException {
        final SaveResult[] saveResultArray = partnerConnection.update(sObjects.toArray(new SObject[sObjects.size()]));
        loggingSaveErrorMessage(saveResultArray);
    }

    private void loggingSaveErrorMessage(final SaveResult[] saveResultArray)
    {
        final List<SaveResult> saveResults = Arrays.asList(saveResultArray);
        saveResults.forEach(result -> {
            if (!result.isSuccess()) {
                final List<String> errors = Arrays.asList(result.getErrors())
                                            .stream().map(e -> e.getStatusCode() + ":" + e.getMessage())
                                            .collect(Collectors.toList());
                logger.warn(String.join(",", errors));
            }
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
