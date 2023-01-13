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
import org.embulk.spi.time.Timestamp;
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
    final String exception =
        String.format("%s(%s)", fault.getExceptionCode(), fault.getExceptionMessage());
    sObjects.forEach(sObject -> log(sObject, exception));
    return sObjects.size();
  }

  private void log(final SObject sObject, final String exception) {
    logger.warn(String.format("object:%s,exception:%s", getObject(sObject), exception));
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
    logger.warn(String.format("object:%s,errors:%s", getObject(sObject), getErrors(result)));
  }

  private String getObject(final SObject sObject) {
    final Map<String, Object> map = new LinkedHashMap<>();
    schema.getColumns().forEach(column -> map.put(column.getName(), getField(sObject, column)));
    return GSON.toJson(map);
  }

  private Object getField(final SObject sObject, final Column column) {
    final Object field = sObject.getField(column.getName());
    if (field == null) {
      return null;
    }
    final String type = column.getType().getName();
    if ("timestamp".equals(type)) {
      return Timestamp.ofInstant(((Calendar) field).toInstant()).toString();
    } else if ("boolean".equals(type)) {
      return Boolean.valueOf(field.toString());
    } else if ("double".equals(type)) {
      return Double.valueOf(field.toString());
    } else if ("long".equals(type)) {
      return Long.valueOf(field.toString());
    } else {
      return field.toString();
    }
  }

  private String getErrors(final Result result) {
    final Map<String, Object> map = new LinkedHashMap<>();
    Arrays.stream(result.getErrors())
        .forEach(
            error ->
                map.put(
                    String.join(",", error.getFields()),
                    String.format("%s(%s)", error.getStatusCode(), error.getMessage())));
    return GSON.toJson(map);
  }

  private interface Result {
    boolean isFailure();

    IError[] getErrors();
  }
}
