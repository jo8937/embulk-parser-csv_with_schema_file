package org.embulk.parser.csv_with_schema_file;

import static org.junit.Assume.assumeNotNull;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.parser.csv_with_schema_file.CsvParserWithSchemaFilePlugin.BqPluginTask;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputRunner;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.type.Types;
import org.embulk.standards.CsvParserPlugin;
import org.embulk.standards.CsvParserPlugin.PluginTask;
import org.embulk.standards.LocalFileInputPlugin;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

public class TestCsvParserWithSchemaFilePlugin
{
	private static final Logger log = LoggerFactory.getLogger(TestCsvParserWithSchemaFilePlugin.class);

    @BeforeClass
    public static void initializeConstantVariables()
    {
    }
    
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    protected ConfigSource config;
    protected CsvParserWithSchemaFilePlugin plugin;
    protected FileInputRunner runner;
    protected MockPageOutput output;
    
    /**
     * @throws IOException 
     * 
     */
    @Before
    public void createResources() throws IOException 
    {
    	config = Exec.newConfigSource();
    	config.set("schema_path", "D:\\Temp\\gcstemp2\\csv_schema.json");
    	config.set("default_timestamp_format", "%Y-%m-%d %H:%M:%S %z");
    	//config.set("columns","");
    	        
        plugin = new CsvParserWithSchemaFilePlugin();
        runner = new FileInputRunner(runtime.getInstance(LocalFileInputPlugin.class));
        output = new MockPageOutput();

        assumeNotNull(
                      config.get(String.class, "schema_path")
                      );
    }
    
    @Test
    public void testParseSchema(){
    	BqPluginTask task = config.loadConfig(BqPluginTask.class);
    	Schema schema = plugin.getSchemaFromFile(task.getSchemaPath());
    	log.info("{}",schema.getColumns());
    	assumeNotNull(schema.getColumns());
    }
    
    public String toJson(Object o){
		ObjectMapper mapper = new ObjectMapper();
		try {
			String jsonString = mapper.writeValueAsString(o);
			return jsonString;
		} catch (JsonProcessingException e) {
			log.error("error when create schema json", e);
			return null;
		}
    }
    @Test
    public void testColumnConfig(){
    	List<ColumnConfig> l = Lists.newArrayList();
    	//ColumnConfig c = new ColumnConfig(config);
    	ConfigSource emptySource = Exec.newConfigSource();
    	
    	l.add( new ColumnConfig("aa",Types.LONG, emptySource));
    	l.add( new ColumnConfig("bb",Types.STRING, emptySource));
    	l.add( new ColumnConfig("cc",Types.TIMESTAMP, "%Y-%m-%d %H:%M:%S"));
    	
    	log.info("list : {}", toJson(l));
    	
    	SchemaConfig con = new SchemaConfig(l);
    	log.info("SchemaConfig : {}", toJson(con));
    	
    	log.info("{}", toJson(con.toSchema()) );
    	log.info("{}", con.toSchema().getColumns());
    }
    

    @Test
    public void testInit(){
    	BqPluginTask task = config.loadConfig(BqPluginTask.class);
    	plugin.transaction(config, new ParserPlugin.Control() {
			
			@Override
			public void run(TaskSource taskSource, Schema schema) {
				log.info("run test {} : {}");
			}
		});
    	CsvParserPlugin.PluginTask parents = config.loadConfig(CsvParserPlugin.PluginTask.class);
    	log.info("{}", parents);
    }
    

    @Test
    public void testParserDefaultrConfig() throws IOException{
    	File f = new File("D:\\temp\\embulk_test.yml");
    	ConfigSource cpn = new ConfigLoader(Exec.session().getModelManager()).fromYamlFile(f).getNested("in").getNested("parser");
    	PluginTask task = cpn.loadConfig(PluginTask.class);
    	log.info("values : {}",task);
    }
}
