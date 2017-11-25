package org.embulk.parser.csv_with_schema_file;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.type.Type;
import org.embulk.standards.CsvParserPlugin;
import org.jruby.org.objectweb.asm.TypeReference;
import org.slf4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class CsvParserWithSchemaFilePlugin
        extends CsvParserPlugin
{

	private static final Logger log = Exec.getLogger(CsvParserWithSchemaFilePlugin.class);
	
	public interface BqPluginTask extends PluginTask
	{
		@Config("schema_path")
		String getSchemaPath();
		
		@Config("schema_class")
		@ConfigDefault("\"Schema\"")
		String getSchemaClass();
		
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

    	config.set("columns", getSchemaConfig(schemaTask, config));
    	
        super.transaction(config, control);
    }
    
    public static class ColumnConfigTemp
    {
    	
        public ColumnConfigTemp() {
			super();
		}
        
		private String name;
        private Type type;
        private String format;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public Type getType() {
			return type;
		}
		public void setType(Type type) {
			this.type = type;
		}
		public String getFormat() {
			return format;
		}
		public void setFormat(String format) {
			this.format = format;
		}
        
    }
    /**
     * if "columns" in embulk config file, use that 
     * @param schemaTask
     * @param config
     * @return
     */
    public SchemaConfig getSchemaConfig(BqPluginTask schemaTask, ConfigSource config) {
    	Map<String, ColumnConfig> map = Maps.newHashMap();
    	if(schemaTask.getSchemaConfig() != null && schemaTask.getSchemaConfig().isEmpty() == false) {
    		// schemaTask.getSchemaConfig().getColumns().stream().collect(Collectors.toMap(x-> x.getName(), y -> y, (a,b) -> b));
    		for(ColumnConfig c : schemaTask.getSchemaConfig().getColumns()) {
    			map.put(c.getName(), c);
    		}
    	}
    	List<ColumnConfig> columns = Lists.newArrayList();
    	if("SchemaConfig".equals(schemaTask.getSchemaClass().trim())) {
    		//SchemaConfig schema = getSchemaFromFile(schemaTask.getSchemaPath(), SchemaConfig.class);
    		ColumnConfigTemp[] mapList = getSchemaFromFile(schemaTask.getSchemaPath(), ColumnConfigTemp[].class);
    		for(ColumnConfigTemp c : mapList){
    			if(map.containsKey(c.getName())) {
    				columns.add(map.get(c.getName()));
    			}else {
    				columns.add(new ColumnConfig(c.getName(), c.getType(), c.getFormat()));	
    			}
        	}
    	}else {
        	Schema schema = getSchemaFromFile(schemaTask.getSchemaPath(), Schema.class);
        	for(Column c : schema.getColumns()){
    			if(map.containsKey(c.getName())) {
    				columns.add(map.get(c.getName()));
    			}else {
    				columns.add(new ColumnConfig(c.getName(), c.getType(), config));	
    			}
        	}
    	}
    	SchemaConfig conf = new SchemaConfig(columns);

    	log.info("Final Config : {}", conf.toSchema());
    	
    	return conf;
    }
    
    
    
    public <T> T getSchemaFromFile(String path, Class<T> cls) {
    	ObjectMapper mapper = new ObjectMapper();
		try {
			T schema = mapper.readValue(new File(path), cls);
			return schema;
		} catch (Exception e) {
			throw new RuntimeException("error when parse Schema : <"+cls+"> file : " + path,e);
		}
    }
}
