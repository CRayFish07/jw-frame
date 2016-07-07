package com.iisquare.jwframe.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.log4j.Logger;

/**
 * JDBC连接管理类，唯一实例，用于维护连接池
 * @author Ouyang <iisquare@163.com>
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
			if(logger.isDebugEnabled()) logger.debug("Connection pool witch named " + name + " is null!");
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

	public boolean addPool(Connector connector, String dbUrl) {
		if(pools.contains(dbUrl)) return true;
		synchronized (ConnectionManager.class) {
			if(pools.contains(dbUrl)) return true;
			ConnectionPool pool = new ConnectionPool(
					connector.getJdbcDriver(), dbUrl, connector.getUsername(), connector.getPassword());
			Boolean isCheckValid = connector.getIsCheckValid();
			Integer incrementalConnections = connector.getIncrementalConnections();
			Integer decrementalConnections = connector.getDecrementalConnections();
			Integer initialConnections = connector.getInitialConnections();
			Integer maxConnections = connector.getMaxConnections();
			Integer timeEventInterval = connector.getTimeEventInterval();
			if(null != isCheckValid) pool.setCheckValid(isCheckValid);
			pool.setIncrementalConnections(incrementalConnections);
			pool.setDecrementalConnections(decrementalConnections);
			pool.setInitialConnections(initialConnections);
			pool.setMaxConnections(maxConnections);
			pool.setTimeEventInterval(timeEventInterval);
			pools.put(dbUrl, pool);
		}
		return true;
	}
}
