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

  /**
   * Derives the Salesforce relationship name from a reference field API name. The Partner SOAP API
   * requires the relationship name (not the field API name) when setting nested SObjects.
   *
   * <p>Salesforce naming conventions:
   *
   * <ul>
   *   <li>Standard fields: strip trailing "Id" (e.g. AccountId → Account, OwnerId → Owner)
   *   <li>Custom fields: replace "__c" with "__r" (e.g. Company__c → Company__r)
   * </ul>
   *
   * The __c check comes first because a custom field could contain "Id" in its name (e.g. SomeId__c
   * → SomeId__r, not SomeId_).
   */
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
