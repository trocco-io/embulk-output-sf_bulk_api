package org.embulk.output.sf_bulk_api;

import org.embulk.util.config.Config;
import org.embulk.util.config.Task;

public interface AssociationConfig extends Task {
  @Config("reference_field")
  String getReferenceField();

  @Config("referenced_object")
  String getReferencedObject();

  @Config("unique_key")
  String getUniqueKey();

  @Config("source_column")
  String getSourceColumn();

  static String deriveRelationshipName(String referenceField) {
    if (referenceField.endsWith("__c")) {
      return referenceField.substring(0, referenceField.length() - 3) + "__r";
    }
    if (referenceField.endsWith("Id")) {
      return referenceField.substring(0, referenceField.length() - 2);
    }
    return referenceField;
  }
}
