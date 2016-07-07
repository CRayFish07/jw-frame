package com.iisquare.jwframe.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.log4j.Logger;

/**
 * JDBC连接管理类，唯一实例，用于维护连接池
 * @author Ouyang
 *
 */
public class ConnectionManager {
	
	private static ConnectionManager instance; // 唯一实例
	
	private static int clients = 0;
	private static Hashtable<String, ConnectionPool> pools = new Hashtable<>(); // 连接
	private Logger logger = Logger.getLogger(getClass().getName());

	public static int getClients() {
		return clients;
	}

	private ConnectionManager() {}
	
	public static ConnectionManager getInstance() {
		if (null == instance) {
			synchronized(ConnectionManager.class) {
				if(null == instance) instance = new ConnectionManager();
			}
		}
		clients++;
		return instance;
	}
	
	public void returnConnection(String name, Connection con) {
		ConnectionPool pool = pools.get(name);
		if (pool != null) {
			pool.returnConnection(con);
		} else {
			logger.debug("Connection pool witch named " + name + " is null!");
		}
	}

	public Connection getConnection(String name) throws SQLException {
		return getConnection(name, -1);
	}

	/**
	 *  获取JDBC连接
	 * @param name 连接池名称
	 * @param timeout never timeout when the value equals -1
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection(String name, long timeout) throws SQLException {
		ConnectionPool pool = pools.get(name);
		if (pool != null) {
			long startTime = System.currentTimeMillis();
			Connection con;
			while ((con = pool.getConnection()) == null) {
				try {
					Thread.sleep(250);
				} catch (InterruptedException e) {}
				if (timeout >= 0 && (System.currentTimeMillis() - startTime) >= timeout) {
					return null;
				}
			}
			return con;
		}
		return null;
	}
	
	public synchronized void release() throws SQLException {
		if (--clients != 0) return;
		Enumeration<ConnectionPool> allPools = pools.elements();
		while (allPools.hasMoreElements()) {
			ConnectionPool pool = (ConnectionPool) allPools.nextElement();
			pool.closeConnectionPool();
		}
	}
	
	public void printDebugMsg(String name) {
		ConnectionPool pool = pools.get(name);
		if (pool != null) {
			pool.printDebugMsg("DBConnectionPool[" + name + "] ", "");
		}
	}

	/*public synchronized boolean init(String path) {
		if(clients > 1) return false;
		DomXmlBuilder xmlBuilder = null;
		Element root = null;
		Element[] tempElements = null;
		xmlBuilder = new DomXmlBuilder(path);
		root = xmlBuilder.getRoot();
		tempElements = DomXmlOperator.getElementsByName(root, "connectionPool");
		for (int i = 0; i < tempElements.length; i++) {
			Element[] tes = null;
			tes = DomXmlOperator.getElementsByName(tempElements[i], "poolName");
			String poolName = DomXmlOperator.getElementValue(tes[0]);
			
			tes = DomXmlOperator.getElementsByName(tempElements[i], "jdbcDriver");
			String jdbcDriver = DomXmlOperator.getElementValue(tes[0]);
			
			tes = DomXmlOperator.getElementsByName(tempElements[i], "dbUrl");
			String dbUrl = DomXmlOperator.getElementValue(tes[0]);
			
			tes = DomXmlOperator.getElementsByName(tempElements[i], "dbUsername");
			String dbUsername = DomXmlOperator.getElementValue(tes[0]);
			
			tes = DomXmlOperator.getElementsByName(tempElements[i], "dbPassword");
			String dbPassword = DomXmlOperator.getElementValue(tes[0]);
			
			tes = DomXmlOperator.getElementsByName(tempElements[i], "bCheckConnection");
			boolean bCheckConnection = CharaterHandle.string2Boolean(DomXmlOperator.getElementValue(tes[0]));
			
			tes = DomXmlOperator.getElementsByName(tempElements[i], "usingIsValid");
			boolean usingIsValid = CharaterHandle.string2Boolean(DomXmlOperator.getElementValue(tes[0]));
			
			tes = DomXmlOperator.getElementsByName(tempElements[i], "testTable");
			String testTable = DomXmlOperator.getElementValue(tes[0]);
			
			tes = DomXmlOperator.getElementsByName(tempElements[i], "initialConnections");
			int initialConnections = Integer.parseInt(DomXmlOperator.getElementValue(tes[0]));
			
			tes = DomXmlOperator.getElementsByName(tempElements[i], "incrementalConnections");
			int incrementalConnections = Integer.parseInt(DomXmlOperator.getElementValue(tes[0]));
			
			tes = DomXmlOperator.getElementsByName(tempElements[i], "decrementalConnections");
			int decrementalConnections = Integer.parseInt(DomXmlOperator.getElementValue(tes[0]));
			
			tes = DomXmlOperator.getElementsByName(tempElements[i], "maxConnections");
			int maxConnections = Integer.parseInt(DomXmlOperator.getElementValue(tes[0]));
			
			tes = DomXmlOperator.getElementsByName(tempElements[i], "timeEventInterval");
			long timeEventInterval = Long.parseLong(DomXmlOperator.getElementValue(tes[0]));
			
			DBConnectionPool pool = new DBConnectionPool(jdbcDriver, dbUrl, dbUsername, dbPassword);
			pool.setbCheckConnection(bCheckConnection);
			pool.setIncrementalConnections(incrementalConnections);
			pool.setDecrementalConnections(decrementalConnections);
			pool.setInitialConnections(initialConnections);
			pool.setMaxConnections(maxConnections);
			pool.setUsingIsValid(usingIsValid);
			pool.setTestTable(testTable);
			pool.setTimeEventInterval(timeEventInterval);
			pools.put(poolName, pool);
		}
		return true;
	}*/
}
