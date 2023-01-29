package org.smcql.config;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.smcql.executor.config.WorkerConfiguration;
import org.smcql.executor.smc.OperatorExecution;
//import org.smcql.executor.smc.OperatorExecution;
import org.smcql.util.Utilities;

import javax.sql.DataSource;

/**
 * 根据配置文件获取了诚实代理的地址、初始化logger、（初始化了calcite的connect、schema、parser）
 */

public class SystemConfiguration {
    public static SqlDialect DIALECT = SqlDialect.DatabaseProduct.POSTGRESQL.getDialect();

    static SystemConfiguration instance = null;
    private static final Logger logger = Logger.getLogger(SystemConfiguration.class.getName());


    private Map<String, String> config;
    // package name --> SQL statement

    String configFile = null;


    // calcite parameters
    SchemaPlus pdnSchema;
    CalciteConnection calciteConnection;
    FrameworkConfig calciteConfig;

    int operatorCounter = -1;
    int queryCounter = -1;
    int portCounter = 54320;

    protected SystemConfiguration() throws Exception {
        config = new HashMap<String, String>();

        String configStr = System.getProperty("smcql.setup.str");
        if (configStr != null) {
            //在各方服务器执行
            List<String> parameters = Arrays.asList(StringUtils.split(configStr, '\n'));
            parseConfiguration(parameters);
            initializeLogger();
            DIALECT = SqlDialect.getProduct(getProperty("db-current"),"").getDialect();
            return;
        }

        configFile = System.getProperty("smcql.setup");

        if (configFile == null)
            configFile = Utilities.getSMCQLRoot() + "/conf/setup";

        File f = new File(configFile); // 可能并不总是存在于远程调用中 may not always exist in remote invocations
        if (f.exists()) {
            List<String> parameters = Utilities.readFile(configFile);
            parseConfiguration(parameters);
        }
        DIALECT = SqlDialect.getProduct(getProperty("db-current"),"").getDialect();
        initializeLogger();
        initializeCalcite();
    }

    private void initializeLogger() throws SecurityException, IOException {
        String filename = config.get("log-file");
        if (filename == null)
            filename = "smcql.log";

        String logFile = Utilities.getSMCQLRoot() + "/" + filename;


        String logLevel = config.get("log-level");
        if (logLevel != null)
            logLevel = logLevel.toLowerCase();


        logger.setUseParentHandlers(false);

        if (logLevel != null && logLevel.equals("debug")) {
            logger.setLevel(Level.FINE);
        } else if (logLevel != null && logLevel.equals("off")) {
            logger.setLevel(Level.OFF);
        } else {
            logger.setLevel(Level.INFO);
        }

        SimpleFormatter fmt = new SimpleFormatter();
        StreamHandler sh = new StreamHandler(System.out, fmt);
        logger.addHandler(sh);

        try {
            FileHandler handler = new FileHandler(logFile);

            SimpleFormatter formatter = new SimpleFormatter();
            handler.setFormatter(formatter);

            logger.addHandler(handler);
        } catch (Exception e) { // fall back to home dir
            logFile = "%h/smcql.log";
            FileHandler handler = new FileHandler(logFile);
            SimpleFormatter formatter = new SimpleFormatter();
            handler.setFormatter(formatter);

            logger.addHandler(handler);

            // show in console to verify
            logger.setUseParentHandlers(true);

        }


    }

    void initializeCalcite() throws ClassNotFoundException, SQLException {
        Properties props = new Properties();
        props.setProperty("caseSensitive", "false");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
        calciteConnection = connection.unwrap(CalciteConnection.class);

        // check driver exist, please append here
        Class.forName("com.mysql.cj.jdbc.Driver");
        Class.forName("org.postgresql.Driver");
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

        DataSource dataSource;
        String catalogStr = null;
        String schemaStr = null;
        switch (getProperty("db-current")) {
            case "mysql":
                MysqlDataSource mysqlDataSource = new MysqlDataSource();
                mysqlDataSource.setUrl("jdbc:mysql://" + getProperty("mysql-host") + ":" + getProperty("mysql-port") + "/" + getProperty("mysql-db"));
                mysqlDataSource.setUser(getProperty("mysql-user"));
                mysqlDataSource.setPassword(getProperty("mysql-password"));
                catalogStr = getProperty("mysql-db");
                dataSource = mysqlDataSource;
                break;
            case "sqlserver":
                SQLServerDataSource sqlserverDataSource = new SQLServerDataSource();
                sqlserverDataSource.setURL("jdbc:sqlserver://" + getProperty("sqlserver-host") + ":" + getProperty("sqlserver-port") + ";DatabaseName=" + getProperty("sqlserver-db"));
                sqlserverDataSource.setUser(getProperty("sqlserver-user"));
                sqlserverDataSource.setPassword(getProperty("sqlserver-password"));
                catalogStr = getProperty("sqlserver-db");
                dataSource = sqlserverDataSource;
                break;
            default:
                BasicDataSource basicDataSource = new BasicDataSource();
                basicDataSource.setUrl("jdbc:postgresql://" + getProperty("postgres-host") + ":" + getProperty("postgres-port") + "/" + getProperty("postgres-db"));
                basicDataSource.setUsername(getProperty("postgres-user"));
                basicDataSource.setPassword(getProperty("postgres-password"));
                dataSource = basicDataSource;
                break;
        }

        JdbcSchema schema = JdbcSchema.create(calciteConnection.getRootSchema(), "name", dataSource, catalogStr, schemaStr);

        for (String tableName : schema.getTableNames()) {//底层调用PgDatabaseMetaData.getTables，通过sql查到了数据库的所有schema，mysql需要指定到具体数据库
            Table table = schema.getTable(tableName);
            calciteConnection.getRootSchema().add(tableName, table);
        }
        pdnSchema = calciteConnection.getRootSchema();//TODO tians 后续支持同时连接多个不同类型数据库，与诚实代理的配置分离。需要后续执行部分支持...

        Config parserConf = SqlParser.configBuilder().setCaseSensitive(false).setLex(Lex.SQL_SERVER).build();
        calciteConfig = Frameworks.newConfigBuilder().defaultSchema(pdnSchema).parserConfig(parserConf).build();

    }


    public static SystemConfiguration getInstance() throws Exception {
        if (instance == null) {
            System.out.println("实例化系统配置");
            instance = new SystemConfiguration();
        }
        return instance;
    }

    public Logger getLogger() {
        return logger;
    }

    public void setProperty(String key, String value) {
        config.put(key, value);
    }


    public String getConfigFile() {
        return configFile;
    }

    private void parseConfiguration(List<String> parameters) {
        String prefix = null;

        for (String p : parameters) {
            if (!p.startsWith("#")) { // skip comments
                if (p.startsWith("[") && p.endsWith("]")) {
                    prefix = p.substring(1, p.length() - 1);
                } else if (p.contains("=")) {
                    String[] tokens = p.split("=");
                    String key = tokens[0];
                    String value = (tokens.length > 1) ? tokens[1] : null;
                    if (prefix != null) {
                        key = prefix + "-" + key;
                    }

                    config.put(key, value);

                }

            }
        }

    }


    public String getProperty(String p) {
        return config.get(p);
    }

    public String getOperatorId() {
        ++operatorCounter;
        return String.valueOf(operatorCounter);
    }

    public String getQueryName() {
        ++queryCounter;

        return "query" + queryCounter;
    }

    public WorkerConfiguration getHonestBrokerConfig() throws ClassNotFoundException, SQLException {
        /// public WorkerConfiguration(String worker,
        //String h, int p, String dbName, String user, String pass)  // psql
        String currentDB = config.get("db-current");
        String host = config.get(currentDB + "-host");
        int port = Integer.parseInt(config.get(currentDB + "-port"));
        String dbName = config.get(currentDB + "-db");
        String user = config.get(currentDB + "-user");
        String pass = config.get(currentDB + "-password");


        return new WorkerConfiguration("honest-broker", host, port, dbName, user, pass);
    }

    public FrameworkConfig getCalciteConfiguration() {
        return calciteConfig;
    }

    public SchemaPlus getPdnSchema() {
        return pdnSchema;
    }

    public CalciteConnection getCalciteConnection() {
        return calciteConnection;
    }

    public int readAndIncrementPortCounter() {
        ++portCounter;
        return portCounter;
    }

    public void resetCounters() {
        operatorCounter = -1;
        queryCounter = -1;
        portCounter = 54320;
    }
}
