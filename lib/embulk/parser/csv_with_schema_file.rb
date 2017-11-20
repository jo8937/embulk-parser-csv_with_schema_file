Embulk::JavaPlugin.register_parser(
  "csv_with_schema_file", "org.embulk.parser.csv_with_schema_file.CsvParserWithSchemaFilePlugin",
  File.expand_path('../../../../classpath', __FILE__))
