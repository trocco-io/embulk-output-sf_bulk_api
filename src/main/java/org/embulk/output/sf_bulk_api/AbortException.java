package org.embulk.output.sf_bulk_api;

public class AbortException extends RuntimeException {
  public AbortException(final Throwable cause) {
    super(cause);
  }
}
