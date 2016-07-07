package com.iisquare.jwframe.database;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import com.iisquare.jwframe.utils.DPUtil;
import com.iisquare.jwframe.utils.PropertiesUtil;

public abstract class Connector {
	
	private static Map<String, Map<String, Object>> config;
	
    protected static Map<String, Object> loadConfig(String dbType) {
        if(null == config) {
        	synchronized (Connector.class) {
        		if(null == config) {
        			config = new Hashtable<>();
        			Properties prop = PropertiesUtil.load(Connector.class.getClassLoader(), "jdbc.properties");
        			if(!DPUtil.empty(prop.getProperty(dbType))) {
        				config.put(dbType, loadMySQLConfig(prop));
        			}
        		}
        	}
        }
        if(null == dbType) return null;
        return config.get(dbType);
    }
    
    private static Map<String, Object> loadMySQLConfig(Properties prop) {
    	Map<String, Object> configMap = new Hashtable<>();
    	configMap.put("dbname", prop.getProperty("dbname"));
    	configMap.put("password", prop.getProperty("password"));
    	configMap.put("charset", prop.getProperty("charset"));
    	configMap.put("tablePrefix", prop.getProperty("tablePrefix"));
    	String[] stringArray;
    	// 主库配置
    	String master = prop.getProperty("master");
    	Map<String, Object> masterMap = new Hashtable<>();
    	stringArray = DPUtil.explode(master, ":", " ", false);
    	if(2 == stringArray.length) {
    		masterMap.put("host", stringArray[0]);
    		masterMap.put("port", stringArray[1]);
    	}
    	configMap.put("master", master);
    	// 从库配置
    	String slaves = prop.getProperty("slaves");
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
