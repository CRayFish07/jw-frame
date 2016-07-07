package com.iisquare.jwframe.database;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import com.iisquare.jwframe.utils.DPUtil;
import com.iisquare.jwframe.utils.PropertiesUtil;

/**
 * 连接器基类
 * @author Ouyang <iisquare@163.com>
 *
 */
public abstract class Connector {
	
	public static final String DBTYPE_MYSQL = "mysql";
	public static final String JDBCDRIVER_MYSQL = "com.mysql.jdbc.Driver";
	private static Map<String, Map<String, Object>> config;
	
	protected String jdbcDriver;
	protected String username;
	protected String password;
	protected Boolean isCheckValid;
	protected Integer incrementalConnections;
	protected Integer decrementalConnections;
	protected Integer initialConnections;
	protected Integer maxConnections;
	protected Integer timeEventInterval;
	
	public String getJdbcDriver() {
		return jdbcDriver;
	}

	public void setJdbcDriver(String jdbcDriver) {
		this.jdbcDriver = jdbcDriver;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Boolean getIsCheckValid() {
		return isCheckValid;
	}

	public void setIsCheckValid(Boolean isCheckValid) {
		this.isCheckValid = isCheckValid;
	}

	public Integer getIncrementalConnections() {
		return incrementalConnections;
	}

	public void setIncrementalConnections(Integer incrementalConnections) {
		this.incrementalConnections = incrementalConnections;
	}

	public Integer getDecrementalConnections() {
		return decrementalConnections;
	}

	public void setDecrementalConnections(Integer decrementalConnections) {
		this.decrementalConnections = decrementalConnections;
	}

	public Integer getInitialConnections() {
		return initialConnections;
	}

	public void setInitialConnections(Integer initialConnections) {
		this.initialConnections = initialConnections;
	}

	public Integer getMaxConnections() {
		return maxConnections;
	}

	public void setMaxConnections(Integer maxConnections) {
		this.maxConnections = maxConnections;
	}

	public Integer getTimeEventInterval() {
		return timeEventInterval;
	}

	public void setTimeEventInterval(Integer timeEventInterval) {
		this.timeEventInterval = timeEventInterval;
	}

	protected static Map<String, Object> loadConfig(String dbType) {
        if(null == config) {
        	synchronized (Connector.class) {
        		if(null == config) {
        			config = new Hashtable<>();
        			Properties prop = PropertiesUtil.load(Connector.class.getClassLoader(), "jdbc.properties");
        			if(!DPUtil.empty(prop.getProperty(DBTYPE_MYSQL))) {
        				config.put(DBTYPE_MYSQL, loadMySQLConfig(prop));
        			}
        		}
        	}
        }
        if(null == dbType) return null;
        return config.get(dbType);
    }
    
    private static Map<String, Object> loadMySQLConfig(Properties prop) {
    	Map<String, Object> configMap = new Hashtable<>();
    	configMap.put("dbname", prop.getProperty("mysql.dbname"));
    	configMap.put("username", prop.getProperty("mysql.username"));
    	configMap.put("password", prop.getProperty("mysql.password"));
    	configMap.put("charset", prop.getProperty("mysql.charset"));
    	configMap.put("tablePrefix", prop.getProperty("mysql.tablePrefix"));
    	// 连接池配置
		configMap.put("isCheckValid", prop.getProperty("mysql.isCheckValid"));
		configMap.put("incrementalConnections", prop.getProperty("mysql.incrementalConnections"));
		configMap.put("decrementalConnections", prop.getProperty("mysql.decrementalConnections"));
		configMap.put("initialConnections", prop.getProperty("mysql.initialConnections"));
		configMap.put("maxConnections", prop.getProperty("mysql.maxConnections"));
		configMap.put("timeEventInterval", prop.getProperty("mysql.timeEventInterval"));
    	String[] stringArray;
    	// 主库配置
    	String master = prop.getProperty("mysql.master");
    	Map<String, Object> masterMap = new Hashtable<>();
    	stringArray = DPUtil.explode(master, ":", " ", false);
    	if(2 == stringArray.length) {
    		masterMap.put("host", stringArray[0]);
    		masterMap.put("port", stringArray[1]);
    	}
    	configMap.put("master", master);
    	// 从库配置
    	String slaves = prop.getProperty("mysql.slaves");
    	List<Map<String, Object>> slavesList = new Vector<>();
    	for (String slave : DPUtil.explode(slaves, ",", " ", false)) {
    		stringArray = DPUtil.explode(slave, ":", " ", false);
    		if(2 != stringArray.length) continue;
    		Map<String, Object> slaveMap = new Hashtable<>();
    		slaveMap.put("host", stringArray[0]);
    		slaveMap.put("port", stringArray[1]);
    		slavesList.add(slaveMap);
    	}
    	configMap.put("slaves", slavesList);
    	return configMap;
    }
    
}
