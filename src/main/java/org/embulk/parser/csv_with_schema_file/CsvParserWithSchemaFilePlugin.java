package org.embulk.parser.csv_with_schema_file;

import java.io.File;
import java.util.List;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.standards.CsvParserPlugin;
import org.slf4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

public class CsvParserWithSchemaFilePlugin
        extends CsvParserPlugin
{

	private static final Logger log = Exec.getLogger(CsvParserWithSchemaFilePlugin.class);
	
	public interface BqPluginTask extends PluginTask
	{
		@Config("schema_path")
		String getSchemaPath();
		
        @Config("columns")
        @ConfigDefault("[]")
        @Override
        SchemaConfig getSchemaConfig();
	}

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
    	BqPluginTask schemaTask = config.loadConfig(BqPluginTask.class);

    	log.info("default timestamp format : {}", schemaTask.getDefaultTimestampFormat() );

    	config.set("columns", getSchemaConfig(schemaTask.getSchemaPath(), config));
    	
        super.transaction(config, control);
    }
    
    public SchemaConfig getSchemaConfig(String path, ConfigSource config) {
    	List<ColumnConfig> columns = Lists.newArrayList();
    	Schema schema = getSchemaFromFile(path);
    	for(Column c : schema.getColumns()){
    		columns.add(new ColumnConfig(c.getName(), c.getType(), config));
    	}
    	SchemaConfig conf = new SchemaConfig(columns);
    	return conf;
    }
    
    public Schema getSchemaFromFile(String path) {
    	ObjectMapper mapper = new ObjectMapper();
		try {
			Schema schema = mapper.readValue(new File(path), Schema.class);
			return schema;
		} catch (Exception e) {
			throw new RuntimeException("error when parse schema file : " + path,e);
			
		}
    }
}
