package com.iisquare.jw.frame.model;

public abstract class DaoBase {
	protected String idName = "";
	protected String dbName = "";
	protected String tableName = "";

	protected Exception lastException;
	public static boolean isDebug = false;
	
	public String getIdName() {
		return idName;
	}

	public void setIdName(String idName) {
		this.idName = idName;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	protected void setLastException(Exception e) {
		lastException = e;
		if (isDebug) throw new RuntimeException("Database error!", e);
	}

	public Exception getLastException() {
		return lastException;
	}
}
