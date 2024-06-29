package org.embulk.output.sf_bulk_api;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ForceClient */
public class ForceClient {
  private final PartnerConnection partnerConnection;
  private final Logger logger = LoggerFactory.getLogger(ForceClient.class);
  private final ActionType actionType;
  private final String upsertKey;
  private final ErrorHandler errorHandler;
  private final Map<AuthMethod, ConnectorConfigCreater> connectorConfigCreaters = new HashMap<>();

  public ForceClient(final PluginTask pluginTask, final ErrorHandler errorHandler)
      throws ConnectionException {
    setConnectorConfigCreaters(pluginTask);
    ConnectorConfigCreater connectorConfigCreater = connectorConfigCreaters.get(pluginTask.getAuthMethod());
    ConnectorConfig connectorConfig = connectorConfigCreater.createConnectorConfig();
    this.partnerConnection = Connector.newConnection(connectorConfig);
    this.actionType = ActionType.convertActionType(pluginTask.getActionType());
    this.upsertKey = pluginTask.getUpsertKey();
    this.errorHandler = errorHandler;
  }

  public long action(final List<SObject> sObjects) throws ConnectionException {
    logger.info("sObjects size:" + sObjects.size());
    switch (this.actionType) {
      case INSERT:
        return insert(sObjects);
      case UPSERT:
        return upsert(this.upsertKey, sObjects);
      case UPDATE:
        return update(sObjects);
      default:
        throw new AssertionError("Invalid actionType: " + actionType);
    }
  }

  public void logout() throws ConnectionException {
    this.partnerConnection.logout();
  }

  private void setConnectorConfigCreaters(PluginTask pluginTask)
  {
      connectorConfigCreaters.put(AuthMethod.oauth, new OauthConnectorConfigCreater(pluginTask));
      connectorConfigCreaters.put(AuthMethod.user_password, new UserPasswordConnectorConfigCreater(pluginTask));
  }

  private long insert(final List<SObject> sObjects) throws ConnectionException {
    final SaveResult[] saveResultArray =
        partnerConnection.create(sObjects.toArray(new SObject[sObjects.size()]));
    return errorHandler.handleErrors(sObjects, saveResultArray);
  }

  private long upsert(final String key, final List<SObject> sObjects) throws ConnectionException {
    final UpsertResult[] upsertResultArray =
        partnerConnection.upsert(key, sObjects.toArray(new SObject[sObjects.size()]));
    return errorHandler.handleErrors(sObjects, upsertResultArray);
  }

  private long update(final List<SObject> sObjects) throws ConnectionException {
    final SaveResult[] saveResultArray =
        partnerConnection.update(sObjects.toArray(new SObject[sObjects.size()]));
    return errorHandler.handleErrors(sObjects, saveResultArray);
  }

  private enum ActionType {
    INSERT,
    UPSERT,
    UPDATE;

    public static ActionType convertActionType(final String key) {
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
