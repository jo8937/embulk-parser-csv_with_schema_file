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
- **columns**: Optional. If exists, overwrite in schema file that same column name. (hash, default: `[]`)
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

### Schema file example (csv_schema.json)

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

### Custom column option example  

* this option overwrites schema file's field that has same field name.
* this usage is with bigquery input : https://github.com/jo8937/embulk-input-bigquery_extract_files

```yml

in:
  type: file
  path_prefix: /tmp/csv/
  parser:
    type: csv_with_schema_file
    default_timestamp_format: '%Y-%m-%d %H:%M:%S'
    schema_path: /tmp/csv_schema.json
    columns:
      - {name: Date2, type: timestamp, format: '%Y-%m-%d %H:%M:%S.%N %z'}
out: 
  type: stdout
  

```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
