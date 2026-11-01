# OCSF schema

`data_security_finding-1.9.0.schema.json` is the JSON Schema (draft-07) of the OCSF 1.9.0 Data
Security Finding class (`class_uid` 2006), as served by

    https://schema.ocsf.io/schema/1.9.0/classes/data_security_finding?profiles=host

It is stored as served. The OCSF report tests check every event they write against its required
attributes and the enum ids it lists.

The `host` profile is only the way to get a schema that requires the base attributes and nothing
more: the URL without `profiles` applies every profile, which also requires `cloud` and `osint`.
