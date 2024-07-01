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
- **action_type**: Action type (`insert`, `update`, or `upsert`, required)
- **upsert_key**: Name of the external ID field (string, required when `upsert` action, default: `key`)
- **ignore_nulls**: Whether to ignore nulls or set fields to null when column is null (boolean, default: `true`)
- **throw_if_failed**: Whether to throw exception at the end of transaction if there are one or more failures (boolean, default: `true`)

## Example

### `user_passsword`
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

## Build

```
$ ./gradlew gem
```
