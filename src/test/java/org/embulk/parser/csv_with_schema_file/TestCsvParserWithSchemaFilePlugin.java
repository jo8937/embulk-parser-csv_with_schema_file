package org.embulk.parser.csv_with_schema_file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.codehaus.plexus.util.FileUtils;
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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
    	config.set("default_timestamp_format", "%Y-%m-%d %H:%M:%S %z");
    	
    	SchemaConfig con = createSchemaConfigForTest();
    	config.set("schema_path", createSchemaFileForTest(con));
    	        
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
    	Schema schema = plugin.getSchemaFromFile(task.getSchemaPath(), Schema.class);
    	log.info("{}",schema.getColumns());
    	assumeNotNull(schema.getColumns());
    }
    
    public String toJson(Object o){
		ObjectMapper mapper = new ObjectMapper();
		try {
			mapper.enable(SerializationFeature.INDENT_OUTPUT);
			String jsonString = mapper.writeValueAsString(o);
			return jsonString;
		} catch (JsonProcessingException e) {
			log.error("error when create schema json", e);
			return null;
		}
    }
    
    public SchemaConfig createSchemaConfigForTest() {
    	List<ColumnConfig> l = Lists.newArrayList();
    	ConfigSource emptySource = Exec.newConfigSource();
    	l.add( new ColumnConfig("idx",Types.LONG, emptySource));
    	l.add( new ColumnConfig("title",Types.STRING, emptySource));
    	l.add( new ColumnConfig("regdate",Types.TIMESTAMP, "%Y-%m-%d %H:%M:%S"));
    	SchemaConfig con = new SchemaConfig(l);
    	return con;
    }
    
    
    public File createSchemaConfigFileForTest(SchemaConfig con) throws IOException {
    	//con.getColumns().stream().map(x -> x.getConfigSource().getObjectNode()).collect(Collectors.toList());
    	List<ObjectNode> jsonList = Lists.newArrayList();
    	for(ColumnConfig c : con.getColumns()) {
    		jsonList.add( c.getConfigSource().getObjectNode() );
    	}
    	String configString = toJson(jsonList);
    	log.info("SchemaConfig String : {}",configString);
    	    	    	
    	File f = File.createTempFile("embulk-test-schemaconfig", ".json");
		FileUtils.fileWrite(f, configString);
    	f.deleteOnExit();
    	
    	return f;
    }

    public File createSchemaFileForTest(SchemaConfig con) throws IOException {
    	String schemaString = toJson(con.toSchema());
    	log.debug("Schema String : {}",schemaString);
    	File f = File.createTempFile("embulk-test-schema", ".json");
		FileUtils.fileWrite(f, schemaString);
    	f.deleteOnExit();
    	return f;
    }


    @Test
    public void testSchemaFile() throws IOException{
    	BqPluginTask task = config.loadConfig(BqPluginTask.class);
    	SchemaConfig finalconfig = plugin.getSchemaConfig(task,config);
    	log.info("final config : {}",toJson(finalconfig.getColumns()));

    	assertEquals(finalconfig.getColumn(0).getName(), "idx");
    	assertEquals(finalconfig.getColumn(1).getName(), "title");
    	assertEquals(finalconfig.getColumn(2).getName(), "regdate");
    }
    
    @Test
    public void testSchemaClass() throws IOException{
    	SchemaConfig con = createSchemaConfigForTest();    	
    	File f = createSchemaConfigFileForTest(con);
    	
    	ConfigSource c = config.deepCopy();
    	c.set("schema_path",f);
    	c.set("schema_class","SchemaConfig");
    	
    	BqPluginTask task = c.loadConfig(BqPluginTask.class);
    	SchemaConfig finalconfig = plugin.getSchemaConfig(task,c);
    	log.info("final config : {}",toJson(finalconfig.getColumns()));
    }
    
    @Test
    public void testMergeOriginalConfig() throws IOException{
       	List<ColumnConfig> l = Lists.newArrayList();
    	//ColumnConfig c = new ColumnConfig(config);
    	ConfigSource emptySource = Exec.newConfigSource();
    	l.add( new ColumnConfig("regdate",Types.TIMESTAMP, "%Y-%m-%d %H:%M:%S.%H %z"));
    	SchemaConfig originalSchemaConfig = new SchemaConfig(l);
    	
    	SchemaConfig con = createSchemaConfigForTest();    	
    	File f = createSchemaFileForTest(con);
    	
    	ConfigSource c = config.deepCopy();
    	c.set("columns", originalSchemaConfig );
    	c.set("schema_path",f);
    	BqPluginTask task = c.loadConfig(BqPluginTask.class);
    	SchemaConfig finalconfig = plugin.getSchemaConfig(task,config);
    	
    	log.info("final config : {}",toJson(finalconfig));
    	log.info("final config to schema : {}", toJson(finalconfig.toSchema()));
    	
    	assertEquals(finalconfig.getColumn(0).getName(), con.getColumn(0).getName());
    	assertEquals(finalconfig.getColumn(1).getName(), con.getColumn(1).getName());
    	assertEquals(finalconfig.getColumn(2).getName(), con.getColumn(2).getName());
    	
    	assertEquals(finalconfig.getColumn(2).getOption().get(String.class, "format"), "%Y-%m-%d %H:%M:%S.%H %z");
    	assertEquals(con.getColumn(2).getOption().get(String.class, "format"), "%Y-%m-%d %H:%M:%S");
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

    // @Test
    public void testParserDefaultrConfig() throws IOException{
    	File f = new File("D:\\temp\\embulk_test.yml");
    	ConfigSource cpn = new ConfigLoader(Exec.session().getModelManager()).fromYamlFile(f).getNested("in").getNested("parser");
    	PluginTask task = cpn.loadConfig(PluginTask.class);
    	log.info("values : {}",task);
    }
}
