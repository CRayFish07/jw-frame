package com.iisquare.jwframe.database;

import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;

import com.iisquare.jwframe.utils.DPUtil;

/**
 * MySQL连接器管理类
 * 单次请求内唯一，若共用connectors变量，MAP大小将不可控，严重影响性能！
 * 请求结束后需要调用release方法，返还全部connection资源。
 * @author Ouyang <iisquare@163.com>
 *
 */
public class MySQLConnectorManager extends ConnectorManager {

	private Map<String, MySQLConnector> connectors = new Hashtable<>();
	
	public MySQLConnectorManager(ConnectionManager connectionManager) {
		super(connectionManager);
	}

	public MySQLConnector getConnector(String dbName, String charset) {
    	Map<String, Object> config = loadConfig(Connector.DBTYPE_MYSQL); // config为引用对象，不可覆盖
    	if(null == dbName) dbName = DPUtil.parseString(config.get("dbname"));
		if(null == charset) charset = DPUtil.parseString(config.get("charset"));
        String key = "___" + dbName + "___" + charset + "___";
        if(!connectors.containsKey(key)) {
        	synchronized (MySQLConnectorManager.class) {
        		if(!connectors.containsKey(key)) {
        			connectors.put(key, new MySQLConnector(connectionManager, config, dbName, charset));
        		}
        	}
        }
        return connectors.get(key);
    }
    
    public void release() {
    	if(connectors.isEmpty()) return ;
    	synchronized (MySQLConnectorManager.class) {
    		if(connectors.isEmpty()) return ;
    		for (Entry<String, MySQLConnector> entry : connectors.entrySet()) {
    			entry.getValue().close();
    		}
    		connectors.clear();
    	}
    }
}
