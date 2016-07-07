package com.iisquare.jwframe.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import com.iisquare.jwframe.utils.DPUtil;

/**
 * MySQL主从连接管理类
 * @author Ouyang <iisquare@163.com>
 *
 */
public class MySQLConnector extends Connector {

	private String dbname;
	private String charset;
	private String tablePrefix;
	private String masterHost;
    private int masterPort;
    private String slaveHost;
    private int slavePort;
    private Connection masterResource;
    private Connection slaveResource;
    private int transactionLevel = 0;
    private String masterUrl, slaveUrl;
    private Logger logger = Logger.getLogger(getClass().getName());
    
    private static ConnectionManager manager = ConnectionManager.getInstance();
    private static Map<String, MySQLConnector> connectors = new Hashtable<>();
	
    /**
     * 获取数据库表前缀
     */
	public String getTablePrefix() {
		return tablePrefix;
	}

	@SuppressWarnings("unchecked")
	private MySQLConnector(Map<String, Object> config, String dbName, String charset) {
		jdbcDriver = JDBCDRIVER_MYSQL;
		this.dbname = dbName;
		this.charset = charset;
		Object temp;
		temp = config.get("username");
		if(null != temp) username = DPUtil.parseString(temp);
		temp = config.get("password");
		if(null != temp) password = DPUtil.parseString(temp);
		temp = config.get("tablePrefix");
		if(null != temp) tablePrefix = DPUtil.parseString(temp);
		temp = config.get("isCheckValid");
		if(null != temp) isCheckValid = !DPUtil.empty(temp);
		temp = config.get("incrementalConnections");
		if(null != temp) incrementalConnections = DPUtil.parseInt(temp);
		temp = config.get("decrementalConnections");
		if(null != temp) decrementalConnections = DPUtil.parseInt(temp);
		temp = config.get("initialConnections");
		if(null != temp) initialConnections = DPUtil.parseInt(temp);
		temp = config.get("maxConnections");
		if(null != temp) maxConnections = DPUtil.parseInt(temp);
		temp = config.get("timeEventInterval");
		if(null != temp) timeEventInterval = DPUtil.parseInt(temp);
		Map<String, Object> masterMap = (Map<String, Object>) config.get("master");
		masterHost = DPUtil.parseString(masterMap.get("host"));
		masterPort = DPUtil.parseInt(masterMap.get("port"));
		List<Map<String, Object>> slavesList = (List<Map<String, Object>>) config.get("slaves");
		if(slavesList.isEmpty()) {
			slaveHost = masterHost;
			slavePort = masterPort;
		} else {
			Map<String, Object> slaverMap = slavesList.get(new Random().nextInt(slavesList.size()));
			slaveHost =  DPUtil.parseString(slaverMap.get("host"));;
			slavePort = DPUtil.parseInt(slaverMap.get("port"));
		}
		masterUrl = "jdbc:mysql://" + masterHost + ":" + masterPort + "/" + dbname;
		slaveUrl = "jdbc:mysql://" + slaveHost + ":" + slavePort + "/" + dbname;
		if(!DPUtil.empty(this.charset)) {
			masterUrl += "?characterEncoding=" + this.charset;
			slaveUrl += "?characterEncoding=" + this.charset;
		}
	}
	
    public static MySQLConnector getInstance(String dbName, String charset) {
    	Map<String, Object> config = loadConfig(DBTYPE_MYSQL); // config为引用对象，不可覆盖
    	if(null == dbName) dbName = DPUtil.parseString(config.get("dbname"));
		if(null == charset) charset = DPUtil.parseString(config.get("charset"));
        String key = "___" + dbName + "___" + charset + "___";
        if(!connectors.containsKey(key)) {
        	synchronized (MySQLConnector.class) {
        		if(!connectors.containsKey(key)) {
        			connectors.put(key, new MySQLConnector(config, dbName, charset));
        		}
        	}
        }
        return connectors.get(key);
    }
    
    private Connection getResource(boolean isMaster) {
    	String dbUrl = isMaster ? masterUrl : slaveUrl;
        if(!manager.addPool(this, dbUrl)) return null;
        try {
			return manager.getConnection(dbUrl);
		} catch (SQLException e) {
			logger.error(e.getMessage());
			return null;
		}
    }
    
    public void close() {
    	if(null != masterResource) manager.returnConnection(masterUrl, masterResource);
    	if(null != slaveUrl) manager.returnConnection(slaveUrl, slaveResource);
    }
    /*
    public function getMaster() {
        if($this->logger->isDebugEnabled()) {
            $this->logger->debug('MySQLConnection - [master]'.(isset($this->masterResource) ? 'hit' : 'create')
                .':'.'{host:'.$this->masterHost.';port:'.$this->masterProt.';dbname:'.$this->dbname.';dbuser:'.$this->username.'}');
        }
        if(!isset($this->masterResource)) {
            $this->masterResource = $this->getResource(true);
        }
        return $this->masterResource;
    }
    
    public function getSlave() {
        if($this->logger->isDebugEnabled()) {
            $this->logger->debug('MySQLConnection - [slave]'.(isset($this->slaveResource) ? 'hit' : 'create')
                .':'.'{host:'.$this->slaveHost.';port:'.$this->slavePort.';dbname:'.$this->dbname.';dbuser:'.$this->username.'}');
        }
        if(!isset($this->slaveResource)) {
            $this->slaveResource = $this->getResource(false);
        }
        return $this->slaveResource;
    }
    
    public function isTransaction() {
        return $this->transactionLevel > 0 && $this->masterResource && $this->masterResource->inTransaction();
    }
    
    public function beginTransaction() {
        if ($this->logger->isDebugEnabled()) {
            $this->logger->debug("MySQL - Begin transaction by Level:{$this->transactionLevel}");
        }
        if ($this->transactionLevel === 0) {
            $this->transactionLevel = 1;
            return $this->getMaster()->beginTransaction();
        }
        $this->transactionLevel++;
        return true;
    }
    
    public function commit() {
        if (!$this->isTransaction()) {
            return false;
        }
        $this->transactionLevel--;
        if ($this->logger->isDebugEnabled()) {
            $this->logger->debug("Commit transaction by Level:{$this->transactionLevel}");
        }
        if ($this->transactionLevel === 0) {
            return $this->getMaster()->commit();
        }
    }
    
    public function rollBack() {
        if (!$this->isTransaction()) {
            return false;
        }
        $this->transactionLevel--;
        if ($this->transactionLevel === 0) {
            if ($this->logger->isDebugEnabled()) {
                $this->logger->debug("Rollback transaction by Level:{$this->transactionLevel}");
            }
            return $this->getMaster()->rollBack();
        }
    }
    
    public function __destruct() {
        $this->close();
    }*/
	
}
