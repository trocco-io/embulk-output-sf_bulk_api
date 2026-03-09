package org.embulk.output.sf_bulk_api;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SfIdResolver {
  private final Logger logger = LoggerFactory.getLogger(SfIdResolver.class);
  private final PartnerConnection connection;
  private final String objectType;
  private final String updateKey;
  private final ErrorHandler errorHandler;

  public SfIdResolver(
      PartnerConnection connection,
      String objectType,
      String updateKey,
      ErrorHandler errorHandler) {
    this.connection = connection;
    this.objectType = objectType;
    this.updateKey = updateKey;
    this.errorHandler = errorHandler;
  }

  public ResolveResult resolve(List<SObject> records) throws ConnectionException {
    List<SObject> resolved = new ArrayList<>();
    long unresolvedCount = 0;

    // 1. Collect update_key values and check for null keys
    Map<String, List<SObject>> keyToRecords = new LinkedHashMap<>();
    for (SObject record : records) {
      Object keyValue = record.getField(updateKey);
      if (keyValue == null) {
        errorHandler.handleIdResolveError(record, "update_key value is null");
        unresolvedCount++;
        continue;
      }
      keyToRecords.computeIfAbsent(keyValue.toString(), k -> new ArrayList<>()).add(record);
    }

    // 2. Check for duplicate keys in input data (skip all records with duplicate keys)
    Set<String> duplicateInputKeys = new HashSet<>();
    for (Map.Entry<String, List<SObject>> entry : keyToRecords.entrySet()) {
      if (entry.getValue().size() > 1) {
        duplicateInputKeys.add(entry.getKey());
        for (SObject r : entry.getValue()) {
          errorHandler.handleIdResolveError(
              r, "Duplicate update_key value in input: " + updateKey + "=" + entry.getKey());
          unresolvedCount++;
        }
      }
    }
    duplicateInputKeys.forEach(keyToRecords::remove);

    if (keyToRecords.isEmpty()) {
      return new ResolveResult(resolved, unresolvedCount);
    }

    // 3. Query Salesforce for SFIDs
    String soql = buildQuery(keyToRecords.keySet());
    logger.info("Resolving IDs with SOQL: {}", soql);
    QueryResult queryResult = connection.query(soql);

    // 4. Build key -> SFID mapping and count duplicates
    Map<String, String> keyToId = new HashMap<>();
    Map<String, Integer> keyCounts = new HashMap<>();
    processQueryResults(queryResult, keyToId, keyCounts);

    while (!queryResult.isDone()) {
      queryResult = connection.queryMore(queryResult.getQueryLocator());
      processQueryResults(queryResult, keyToId, keyCounts);
    }

    // 5. Set Id on each record
    for (Map.Entry<String, List<SObject>> entry : keyToRecords.entrySet()) {
      String keyValue = entry.getKey();
      List<SObject> recordsForKey = entry.getValue();

      int count = keyCounts.getOrDefault(keyValue, 0);
      if (count == 0) {
        for (SObject r : recordsForKey) {
          errorHandler.handleIdResolveError(r, "No record found for " + updateKey + "=" + keyValue);
          unresolvedCount++;
        }
      } else if (count > 1) {
        for (SObject r : recordsForKey) {
          errorHandler.handleIdResolveError(
              r, "Multiple records found for " + updateKey + "=" + keyValue);
          unresolvedCount++;
        }
      } else {
        String sfId = keyToId.get(keyValue);
        for (SObject r : recordsForKey) {
          r.setId(sfId);
          resolved.add(r);
        }
      }
    }

    return new ResolveResult(resolved, unresolvedCount);
  }

  private void processQueryResults(
      QueryResult queryResult, Map<String, String> keyToId, Map<String, Integer> keyCounts) {
    for (SObject result : queryResult.getRecords()) {
      Object fieldValue = result.getField(updateKey);
      if (fieldValue == null) {
        continue;
      }
      String key = fieldValue.toString();
      keyCounts.merge(key, 1, Integer::sum);
      keyToId.put(key, result.getId());
    }
  }

  private String buildQuery(Set<String> keyValues) {
    String inClause =
        keyValues.stream().map(v -> "'" + escapeSoql(v) + "'").collect(Collectors.joining(","));
    return String.format(
        "SELECT Id, %s FROM %s WHERE %s IN (%s)", updateKey, objectType, updateKey, inClause);
  }

  private String escapeSoql(String value) {
    return value.replace("\\", "\\\\").replace("'", "\\'");
  }

  public static class ResolveResult {
    private final List<SObject> resolvedRecords;
    private final long unresolvedCount;

    public ResolveResult(List<SObject> resolvedRecords, long unresolvedCount) {
      this.resolvedRecords = resolvedRecords;
      this.unresolvedCount = unresolvedCount;
    }

    public List<SObject> getResolvedRecords() {
      return resolvedRecords;
    }

    public long getUnresolvedCount() {
      return unresolvedCount;
    }
  }
}
