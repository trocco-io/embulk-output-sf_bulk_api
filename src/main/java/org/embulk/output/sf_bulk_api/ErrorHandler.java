package org.embulk.output.sf_bulk_api;

import com.google.gson.Gson;
import com.sforce.soap.partner.IError;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.soap.partner.fault.ExceptionCode;
import com.sforce.soap.partner.sobject.SObject;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorHandler {
  private static final List<ExceptionCode> ABORT_EXCEPTION_CODES =
      Collections.unmodifiableList(
          Arrays.asList(
              ExceptionCode.INVALID_SESSION_ID,
              ExceptionCode.INVALID_OPERATION_WITH_EXPIRED_PASSWORD));
  private static final Gson GSON = new Gson();

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final Schema schema;

  public ErrorHandler(final Schema schema) {
    this.schema = schema;
  }

  public long handleFault(final List<SObject> sObjects, final ApiFault fault) {
    if (ABORT_EXCEPTION_CODES.contains(fault.getExceptionCode())) {
      throw new AbortException(fault); // Abort immediately
    }
    sObjects.forEach(sObject -> log(sObject, fault));
    return sObjects.size();
  }

  private void log(final SObject sObject, final ApiFault fault) {
    logger.error(String.format("[output sf_bulk_api failure] %s", getFailure(sObject, fault)));
  }

  private String getFailure(final SObject sObject, final ApiFault fault) {
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("object", getObject(sObject));
    map.put("errors", getErrors(fault));
    return GSON.toJson(map);
  }

  private List<Map<String, Object>> getErrors(final ApiFault fault) {
    return Arrays.stream(new ApiFault[] {fault}).map(this::getError).collect(Collectors.toList());
  }

  private Map<String, Object> getError(final ApiFault fault) {
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("code", fault.getExceptionCode());
    map.put("message", fault.getExceptionMessage());
    return map;
  }

  public long handleErrors(final List<SObject> sObjects, final SaveResult[] results) {
    return handleErrors(
        sObjects,
        Arrays.stream(results)
            .map(
                result ->
                    new Result() {
                      @Override
                      public boolean isFailure() {
                        return !result.isSuccess();
                      }

                      @Override
                      public IError[] getErrors() {
                        return result.getErrors();
                      }
                    })
            .collect(Collectors.toList()));
  }

  public long handleErrors(final List<SObject> sObjects, final UpsertResult[] results) {
    return handleErrors(
        sObjects,
        Arrays.stream(results)
            .map(
                result ->
                    new Result() {
                      @Override
                      public boolean isFailure() {
                        return !result.isSuccess();
                      }

                      @Override
                      public IError[] getErrors() {
                        return result.getErrors();
                      }
                    })
            .collect(Collectors.toList()));
  }

  private long handleErrors(final List<SObject> sObjects, final List<Result> results) {
    if (sObjects.size() != results.size()) {
      throw new IllegalArgumentException(
          String.format("%d != %d", sObjects.size(), results.size()));
    }
    IntStream.range(0, sObjects.size())
        .forEach(index -> log(sObjects.get(index), results.get(index)));
    return results.stream().filter(Result::isFailure).count();
  }

  private void log(final SObject sObject, final Result result) {
    if (!result.isFailure()) {
      return;
    }
    logger.error(String.format("[output sf_bulk_api failure] %s", getFailure(sObject, result)));
  }

  private String getFailure(final SObject sObject, final Result result) {
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("object", getObject(sObject));
    map.put("errors", getErrors(result));
    return GSON.toJson(map);
  }

  private Map<String, Object> getObject(final SObject sObject) {
    final Map<String, Object> map = new LinkedHashMap<>();
    schema.getColumns().forEach(column -> map.put(column.getName(), getField(sObject, column)));
    return map;
  }

  @SuppressWarnings("deprecation") // For the use of org.embulk.spi.time.Timestamp.
  private Object getField(final SObject sObject, final Column column) {
    final Object field = sObject.getField(column.getName());
    if (field == null) {
      return null;
    }
    final String type = column.getType().getName();
    if ("timestamp".equals(type)) {
      return org.embulk.spi.time.Timestamp.ofInstant(((Calendar) field).toInstant()).toString();
    } else if ("boolean".equals(type)) {
      return Boolean.valueOf(field.toString());
    } else if ("double".equals(type) || "long".equals(type)) {
      return Double.valueOf(field.toString());
    } else {
      return field.toString();
    }
  }

  private List<Map<String, Object>> getErrors(final Result result) {
    return Arrays.stream(result.getErrors()).map(this::getError).collect(Collectors.toList());
  }

  private Map<String, Object> getError(final IError error) {
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("code", error.getStatusCode());
    map.put("message", error.getMessage());
    map.put("fields", error.getFields());
    return map;
  }

  private interface Result {
    boolean isFailure();

    IError[] getErrors();
  }
}
