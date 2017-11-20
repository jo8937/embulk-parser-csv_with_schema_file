# CSV Parser with schema config file plugin for Embulk

Parses csv files with schema file read by other file input plugins.

## Overview

* **Plugin type**: parser
* **Guess supported**: no

## Usage

### Install plugin

```bash
$ embulk gem install embulk-parser-csv_with_schema_file
```

## Configuration

- **schema_path**: schema file path. json. (string, required)
- **columns**: this config is ignored in this plugin.
- other configs extends csv parser. see : http://www.embulk.org/docs/built-in.html#csv-parser-plugin

## Example

```yaml
in:
  type: file
  path_prefix: /tmp/csv/
  parser:
    type: csv_with_schema_file
    schema_path: /tmp/csv_schema.json
    default_timestamp_format: '%Y-%m-%d %H:%M:%S %z'
out: 
  type: stdout
```

* csv_schema.json example

```json
[
   {
      "index": 0,
      "name": "Name",
      "type": "string"
   },
   {
      "index": 1,
      "name": "Cnt",
      "type": "long"
   },
   {
      "index": 2,
      "name": "RegDate",
      "type": "timestamp"
   }
]
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
