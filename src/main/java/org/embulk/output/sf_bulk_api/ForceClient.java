package org.embulk.output.sf_bulk_api;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ForceClient */
public class ForceClient {
  private final PartnerConnection partnerConnection;
  private final Logger logger = LoggerFactory.getLogger(ForceClient.class);
  private final ActionType actionType;
  private final String upsertKey;
  private final String deleteKey;
  private final ErrorHandler errorHandler;
  private final SfIdResolver sfIdResolver;
  private final Map<AuthMethod, ConnectorConfigCreator> connectorConfigCreators = new HashMap<>();

  public ForceClient(final PluginTask pluginTask, final ErrorHandler errorHandler)
      throws ConnectionException {
    setConnectorConfigCreators(pluginTask);
    ConnectorConfigCreator connectorConfigCreator =
        connectorConfigCreators.get(pluginTask.getAuthMethod());
    ConnectorConfig connectorConfig = connectorConfigCreator.createConnectorConfig();
    this.partnerConnection = Connector.newConnection(connectorConfig);
    this.actionType = ActionType.convertActionType(pluginTask.getActionType());
    this.upsertKey = pluginTask.getUpsertKey();
    this.deleteKey = pluginTask.getDeleteKey();
    this.errorHandler = errorHandler;

    if (this.actionType == ActionType.UPDATE && pluginTask.getUpdateKey().isPresent()) {
      this.sfIdResolver =
          new SfIdResolver(
              this.partnerConnection,
              pluginTask.getObject(),
              pluginTask.getUpdateKey().get(),
              errorHandler);
    } else {
      this.sfIdResolver = null;
    }
  }

  public long action(final List<SObject> sObjects) throws ConnectionException {
    logger.info("sObjects size:" + sObjects.size());
    switch (this.actionType) {
      case INSERT:
        return insert(sObjects);
      case UPSERT:
        return upsert(this.upsertKey, sObjects);
      case UPDATE:
        if (sfIdResolver != null) {
          return updateWithExternalKey(sObjects);
        }
        return update(sObjects);
      case DELETE:
        return delete(sObjects);
      default:
        throw new AssertionError("Invalid actionType: " + actionType);
    }
  }

  private long updateWithExternalKey(final List<SObject> sObjects) throws ConnectionException {
    SfIdResolver.ResolveResult resolveResult = sfIdResolver.resolve(sObjects);
    long failures = resolveResult.getUnresolvedCount();
    if (!resolveResult.getResolvedRecords().isEmpty()) {
      failures += update(resolveResult.getResolvedRecords());
    }
    return failures;
  }

  private void setConnectorConfigCreators(PluginTask pluginTask) {
    connectorConfigCreators.put(AuthMethod.oauth, new OauthConnectorConfigCreator(pluginTask));
    connectorConfigCreators.put(
        AuthMethod.user_password, new UserPasswordConnectorConfigCreator(pluginTask));
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

  private long delete(final List<SObject> sObjects) throws ConnectionException {
    // Extract the Salesforce record Id from the delete_key column of each record.
    // Records whose Id is null/empty are counted as failures here (mirroring SfIdResolver)
    // so the records sent to delete() stay aligned 1:1 with the returned DeleteResult[].
    final List<SObject> targets = new ArrayList<>();
    final List<String> ids = new ArrayList<>();
    long failures = 0;
    for (final SObject sObject : sObjects) {
      final Object idValue = sObject.getField(this.deleteKey);
      final String id = idValue == null ? null : idValue.toString().trim();
      if (id == null || id.isEmpty()) {
        errorHandler.handleIdResolveError(
            sObject, "delete_key '" + this.deleteKey + "' value is null or empty");
        failures++;
        continue;
      }
      targets.add(sObject);
      ids.add(id);
    }
    if (ids.isEmpty()) {
      return failures;
    }
    final DeleteResult[] deleteResultArray =
        partnerConnection.delete(ids.toArray(new String[ids.size()]));
    return failures + errorHandler.handleErrors(targets, deleteResultArray);
  }

  private enum ActionType {
    INSERT,
    UPSERT,
    UPDATE,
    DELETE;

    public static ActionType convertActionType(final String key) {
      switch (key) {
        case "insert":
          return INSERT;
        case "upsert":
          return UPSERT;
        case "update":
          return UPDATE;
        case "delete":
          return DELETE;
        default:
          return null;
      }
    }
  }
}
