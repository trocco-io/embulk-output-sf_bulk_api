# embulk-output-sf_bulk_api

Embulk output plugin for Salesforce Bulk API.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no

## Configuration

- **username**: Login username (string, required)
- **password**: Login password (string, required)
- **security_token**: User’s security token (string, required)
- **auth_end_point**: SOAP API authentication endpoint (string, default: `https://login.salesforce.com/services/Soap/u/`)
- **api_version**: SOAP API version (string, default: `46.0`)
- **object**: Salesforce object (sObject) type (string, required)
- **action_type**: Action type (`insert`, `update`, or `upsert`, required)
- **upsert_key**: Name of the external ID field (string, required when `upsert` action, default: `key`)
- **ignore_nulls**: Whether to ignore nulls or set fields to null when column is null (boolean, default: `true`)
- **throw_if_failed**: Whether to throw exception at the end of transaction if there are one or more failures (boolean, default: `true`)

## Example

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

## Build

```
$ ./gradlew gem
```
