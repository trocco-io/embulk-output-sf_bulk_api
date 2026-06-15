# embulk-output-sf_bulk_api

Embulk output plugin for Salesforce Bulk API.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no

## Configuration

- **auth_method**: Select `user_password` or `oauth` (string, default: `user_password`)
- If auth method is `user_password`
  - **username**: Login username (string, required)
  - **password**: Login password (string, required)
  - **security_token**: User’s security token (string, required)
  - **api_version**: SOAP API version (string, default: `46.0`)
  - **auth_end_point**: SOAP API authentication endpoint (string, default: `https://login.salesforce.com/services/Soap/u/`)
- If auth method is `oauth`
  - **server_url**: Oauth server url (string, required)
  - **access_token**: Oauth access token (string, required)
- **object**: Salesforce object (sObject) type (string, required)
- **action_type**: Action type (`insert`, `update`, `upsert`, or `delete`, required)
- **upsert_key**: Name of the external ID field (string, required when `upsert` action, default: `key`)
- **delete_key**: Field that identifies the records to delete. Used only with `delete` action. (string, optional, default: `Id`)
  - When set to `Id` (default), the input column `Id` is read as the Salesforce record Id and deleted directly.
  - When set to any other field, the value is treated as an external/business key: the plugin resolves it to a record Id via SOQL (`SELECT Id, <delete_key> FROM <object> WHERE <delete_key> IN (...)`) and then deletes — the same resolution mechanism as `update_key`. The input column name must match the Salesforce field API name. The field does not need to be a true External ID; any queryable field works. Keys that match zero or multiple records (or are null/duplicated in the input) are reported as failures.
- **ignore_nulls**: Whether to ignore nulls or set fields to null when column is null (boolean, default: `true`)
- **throw_if_failed**: Whether to throw exception at the end of transaction if there are one or more failures (boolean, default: `true`)
- **batch_size**: Number of records per API call (integer, default: `200`, min: `1`, max: `200`)
- **update_key**: Field name to resolve records by external key for `update` action. The plugin queries Salesforce to map the key to record IDs before updating. (string, optional)
- **error_records_detail_output_file**: File path to write detailed error records in JSONL format (string, optional)
- **associations**: List of reference field associations to set via external ID lookup. Salesforce resolves the external ID server-side using nested SObjects — no additional API calls are needed. (list, default: `[]`)
  - **reference_field**: API name of the reference field (e.g. `AccountId`, `Company__c`)
  - **referenced_object**: API name of the referenced object type (e.g. `Account`, `TestCompany__c`)
  - **unique_key**: Field on the referenced object used for matching (e.g. `External_Id__c`)
  - **source_column**: Input column name containing the external ID value

## Example

### `user_password`
```yaml
out:
  type: sf_bulk_api
  username: username
  password: password
  security_token: security_token
  object: ExampleCustomObject__c
  action_type: upsert
  upsert_key: Name
```

### `oauth`
```yaml
out:
  type: sf_bulk_api
  auth_method: oauth
  server_url: server_url
  access_token: access_token
  object: ExampleCustomObject__c
  action_type: upsert
  upsert_key: Name
```

### With associations (external ID reference)
```yaml
out:
  type: sf_bulk_api
  auth_method: oauth
  server_url: server_url
  access_token: access_token
  object: Contact
  action_type: upsert
  upsert_key: Employee_Code__c
  associations:
    - reference_field: AccountId
      referenced_object: Account
      unique_key: External_Id__c
      source_column: account_code
    - reference_field: OwnerId
      referenced_object: User
      unique_key: Username
      source_column: owner_username
```

In this example, the `account_code` input column is used to look up an `Account` by its `External_Id__c` field and set the `AccountId` reference. The `owner_username` column resolves a `User` by `Username` for the polymorphic `OwnerId` field. Salesforce resolves these references server-side within the same API call.

### `delete` (by record Id)
```yaml
out:
  type: sf_bulk_api
  username: username
  password: password
  security_token: security_token
  object: ExampleCustomObject__c
  action_type: delete
  delete_key: Id
```

The input column `Id` is read as the Salesforce record Id and deleted directly.

### `delete` (by external/business key)
```yaml
out:
  type: sf_bulk_api
  username: username
  password: password
  security_token: security_token
  object: ExampleCustomObject__c
  action_type: delete
  delete_key: Employee_Code__c
```

The `Employee_Code__c` column value is resolved to a record Id via SOQL, then deleted — the same resolution mechanism as `update_key`.

Either way the delete is a **logical delete** (moves records to the Recycle Bin), so deleted records remain recoverable. Only the key is used; other input columns are ignored.

Notes:
- Records whose `delete_key` value is null/empty, or (for external keys) that resolve to zero or multiple records, are skipped and counted as failures.
- Re-deleting an already-deleted record returns an `ENTITY_IS_DELETED` error, which is counted as a failure per `throw_if_failed`.
- Physical/hard delete is not supported.

## Build

```
$ ./gradlew gem
```
