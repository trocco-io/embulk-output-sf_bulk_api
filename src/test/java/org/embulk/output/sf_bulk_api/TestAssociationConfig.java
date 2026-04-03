package org.embulk.output.sf_bulk_api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.embulk.config.ConfigException;
import org.junit.Test;

public class TestAssociationConfig {

  // --- deriveRelationshipName: standard fields (end with "Id") ---

  @Test
  public void testDeriveRelationshipNameAccountId() {
    assertEquals("Account", AssociationConfig.deriveRelationshipName("AccountId"));
  }

  @Test
  public void testDeriveRelationshipNameParentId() {
    assertEquals("Parent", AssociationConfig.deriveRelationshipName("ParentId"));
  }

  @Test
  public void testDeriveRelationshipNameOwnerId() {
    assertEquals("Owner", AssociationConfig.deriveRelationshipName("OwnerId"));
  }

  @Test
  public void testDeriveRelationshipNameReportsToId() {
    assertEquals("ReportsTo", AssociationConfig.deriveRelationshipName("ReportsToId"));
  }

  @Test
  public void testDeriveRelationshipNameCreatedById() {
    assertEquals("CreatedBy", AssociationConfig.deriveRelationshipName("CreatedById"));
  }

  @Test
  public void testDeriveRelationshipNameLastModifiedById() {
    assertEquals("LastModifiedBy", AssociationConfig.deriveRelationshipName("LastModifiedById"));
  }

  // --- deriveRelationshipName: custom fields (end with "__c") ---

  @Test
  public void testDeriveRelationshipNameCustomField() {
    assertEquals("My_Account__r", AssociationConfig.deriveRelationshipName("My_Account__c"));
  }

  @Test
  public void testDeriveRelationshipNameCustomFieldAnother() {
    assertEquals("Custom_Field__r", AssociationConfig.deriveRelationshipName("Custom_Field__c"));
  }

  @Test
  public void testDeriveRelationshipNameCustomFieldSimple() {
    assertEquals("Company__r", AssociationConfig.deriveRelationshipName("Company__c"));
  }

  // --- deriveRelationshipName: edge cases ---

  @Test
  public void testDeriveRelationshipNameCustomFieldTakesPrecedenceOverId() {
    // A field ending with "__c" should be treated as custom even if it also contains "Id"
    assertEquals("SomeId__r", AssociationConfig.deriveRelationshipName("SomeId__c"));
  }

  @Test
  public void testDeriveRelationshipNameFallbackThrows() {
    // Neither __c nor Id suffix → ConfigException
    ConfigException e =
        assertThrows(
            ConfigException.class,
            () -> AssociationConfig.deriveRelationshipName("SomeField"));
    assertEquals(
        "Cannot derive relationship name from reference_field 'SomeField'."
            + " Expected a standard field ending with 'Id' or a custom field ending with '__c'.",
        e.getMessage());
  }

  @Test
  public void testDeriveRelationshipNameJustId() {
    // "Id" alone → empty string (edge case, unlikely in practice)
    assertEquals("", AssociationConfig.deriveRelationshipName("Id"));
  }
}
