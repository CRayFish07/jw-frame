package com.iisquare.jwframe.mvc;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.context.WebApplicationContext;

import com.iisquare.jwframe.database.MySQLConnector;
import com.iisquare.jwframe.database.MySQLConnectorManager;
import com.iisquare.jwframe.utils.DPUtil;

public abstract class MySQLBase extends DaoBase {
	
	public static final String PARAM_PREFIX = ":qp";
	@Autowired
	private WebApplicationContext webApplicationContext;
	private MySQLConnectorManager connectorManager;
	private MySQLConnector connector;
	private int retry = 1; // 失败重试次数
	private Connection resource; // 当前连接资源
	private PreparedStatement statement; // 当前预处理对象
	private boolean isMaster = false;
	private String select = "*";
	private String where;
	private String groupBy;
	private String having;
	private String orderBy;
	private Integer limit;
	private Integer offset;
	private List<String[]> join;
	private String sql;
	private Exception exception;
	private Map<String, Object> pendingParams = new LinkedHashMap<>();
	
	public MySQLBase() {}
	
	@PostConstruct
	public boolean reload() {
		if(null == connectorManager) {
			connectorManager = webApplicationContext.getBean(MySQLConnectorManager.class);
		}
		// 不需要调用Connector.close()，全部由ConnectorManager托管
		connector = connectorManager.getConnector(dbName(), charset());
		if(null == connector) return false;
		return true;
	}
	
	/**
	 * 数据库名称，返回空采用配置文件中的默认值
	 */
	public String dbName() {
		return null;
	}

	/**
	 * 数据库编码，返回空采用配置文件中的默认值
	 */
	public String charset() {
		return null;
	}

	/**
	 * 数据库表前缀
	 */
	protected String tablePrefix() {
	    return connector.getTablePrefix();
	}

	/**
	 * 数据库表名称
	 */
	public abstract String tableName();
	
	/**
	 * 设置连接超时时间，0为无限等待
	 */
	public void setTimeout(int timeout) {
		connector.setTimeout(timeout);
	}

	/**
	 * 设置失败重试次数
	 */
	public void setRetry(int retry) {
		this.retry = retry;
	}

	/**
     * 切换表主从模式
     * @param boolean isMaster true 读写全为主库，false 写主读从
     */
    public void setMaster(boolean isMaster) {
        this.isMaster = isMaster;
    }
    
    /**
     * 表字段数组
     */
    public abstract LinkedHashMap<String, Map<String, Object>> columns();
    
    /**
     * 预处理表数据
     */
    public LinkedHashMap<String, Object> prepareData(Map<String, Object> data) {
    	if(null == data) return null;
    	LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    	LinkedHashMap<String, Map<String, Object>> collumnNames = columns();
    	for(Entry<String, Map<String, Object>> entry : collumnNames.entrySet()) {
    		String key = entry.getKey();
    		if(!data.containsKey(key)) continue ;
    		result.put(key, entry.getValue());
    	}
    	return result;
    }
    
    /**
     * 最后一次执行的SQL语句(不含值)
     */
    public String getLastSql() {
        return sql;
    }
    
    /**
     * 最后一次执行的异常，执行成功时不会修改该返回值
     */
    public Exception getLastException() {
        return exception;
    }
    
    private Map<String, Object> buildValues(Object... values) {
    	if(null == values) return null;
    	Map<String, Object> map = new LinkedHashMap<>();
    	for (int i = 0; i < values.length; i += 2) {
			map.put(values[i].toString(), values[i + 1]);
		}
    	return map;
    }
    
    public MySQLBase bindValue(String name, Object value) {
    	pendingParams.put(name, value);
        return this;
    }
	
	public MySQLBase bindValues(Map<String, Object> values) {
		if(null == values) return this;
		for (Entry<String, Object> entry : values.entrySet()) {
			pendingParams.put(entry.getKey(), entry.getValue());
		}
		return this;
	}
    
	public MySQLBase select(String columns) {
        select = columns;
        return this;
    }
	
	public MySQLBase where(String condition, Object... params) {
		return where(condition, buildValues(params));
	}
	
	public MySQLBase where (String condition, Map<String, Object> params) {
		this.where = condition;
		return bindValues(params);
	}

	private MySQLBase join(String type, String table, String on) {
		if(null == join) join = new ArrayList<>();
		join.add(new String[]{type, table, on});
		return this;
	}
	
	public MySQLBase innerJoin(String table, String on, Object... params) {
    	return innerJoin(table, on, buildValues(params));
    }
	
    public MySQLBase innerJoin(String table, String on, Map<String, Object> params) {
    	join("INNER JOIN", table, on);
    	return bindValues(params);
    }
    
    public MySQLBase leftJoin(String table, String on, Object... params) {
    	return leftJoin(table, on, buildValues(params));
    }
    
    public MySQLBase leftJoin(String table, String on, Map<String, Object> params) {
    	join("LEFT JOIN", table, on);
    	return bindValues(params);
    }
    
    public MySQLBase rightJoin(String table, String on, Object... params) {
    	return rightJoin(table, on, buildValues(params));
    }
    
    public MySQLBase rightJoin(String table, String on, Map<String, Object> params) {
    	join("RIGHT JOIN", table, on);
    	return bindValues(params);
    }
    
    public MySQLBase groupBy(String columns) {
        groupBy = columns;
        return this;
    }
    
    public MySQLBase orderBy(String columns) {
        orderBy = columns;
        return this;
    }
    
    public MySQLBase having(String condition, Object... params) {
        return having(condition, buildValues(params));
    }
    
    public MySQLBase having(String condition, Map<String, Object> params) {
        having = condition;
        return bindValues(params);
    }
    
    public MySQLBase limit(int limit) {
        this.limit = limit;
        return this;
    }
    
    public MySQLBase offset(int offset) {
        this.offset = offset;
        return this;
    }
    
    private String build() {
    	StringBuilder sb = new StringBuilder();
    	sb.append("SELECT ").append(null == select ? "*" : select);
    	sb.append(" FROM ").append(tableName());
    	if(null != join) { // JOIN
    		for (String[] item : join) {
    			sb.append(" ").append(item[0]).append(" ").append(item[1]);
    			if(null == item[2]) continue;
    			sb.append(" ON ").append(item[2]);
    		}
    	}
    	if(null != where) sb.append(" WHERE ").append(where);
    	if(null != groupBy) sb.append(" GROUP BY ").append(groupBy);
    	if(null != having) sb.append(" HAVING ").append(having);
    	if(null != orderBy) sb.append(" ORDER BY ").append(orderBy);
    	if(null != offset && null != limit) {
    		sb.append(" LIMIT ");
    		if(null != offset) sb.append(offset).append(", ");
    		sb.append(null == limit ? 0 : limit);
    	}
    	return sb.toString();
    }
    
    public MySQLBase bindParam(int index, Object param) throws SQLException {
        if (null == statement) {
            throw new SQLException("bindParam must exist statement.call method insert update delete query..");
        }
        if(null == param) {
        	statement.setObject(index, param);
        } else if (param instanceof String) {
        	statement.setString(index, param.toString());
		} else if (param instanceof Date) {
			statement.setDate(index, Date.valueOf(param.toString()));
		} else if (param instanceof Boolean) {
			statement.setBoolean(index, (Boolean) (param));
		} else if (param instanceof Integer) {
			statement.setInt(index, (Integer) param);
		} else if (param instanceof Float) {
			statement.setFloat(index, (Float) param);
		} else if (param instanceof Double) {
			statement.setDouble(index, (Double) param);
		} else {
			statement.setObject(index, param);
		}
        return this;
    }
    
    private void bindPendingParams() {
    	for (Entry<String, Object> entry : pendingParams.entrySet()) {
    		// TODO find param index 
    	}
    	pendingParams = new LinkedHashMap<>();
    }
    
    private boolean execute(boolean forRead, int retry) {
        if(null == sql || "".equals(sql)) return false;
        try {
        	if(null != statement) {
            	statement.close();
            	statement = null;
            }
            if(isMaster || connector.isTransaction()) forRead = false;
            resource = forRead ? connector.getSlave() : connector.getMaster();
			statement = resource.prepareStatement(sql);
			bindPendingParams();
	        return statement.execute();
		} catch (SQLException e) {
			exception = e;
			if(retry > 0 && 2006 == e.getErrorCode()) {
				connector.close();
				return execute(forRead, --retry);
			}
			return false;
		}
    }
    
    /**
     * 返回查询的数据资源对象，使用getResultSet()方法遍历数据，如果取出数据后要循环处理，建议使用该方法
    */
	public PreparedStatement query() {
	    sql = build();
	    if(execute(true, retry)) {
	    	return statement;
	    }
	    return null;
	}
	
	/**
	 * 读取ResultSet到List<Map<String, Object>>
	 */
	private List<Map<String, Object>> fetchResultSet(ResultSet rs) throws Exception {
		ResultSetMetaData rsmd = rs.getMetaData();
		List<Map<String, Object>> tempList = new ArrayList<>();
		Map<String, Object> tempHash = null;
		while (rs.next()) {
			tempHash = new LinkedHashMap<String, Object>();
			for (int i = 0; i < rsmd.getColumnCount(); i++) {
				tempHash.put(rsmd.getColumnName(i + 1), rs.getString(rsmd.getColumnName(i + 1)));
			}
			tempList.add(tempHash);
		}
		return tempList;
	}
	
    /**
     * 返回查询的所有数据数组
     */
    public List<Map<String, Object>> all() {
        sql = build();
        if (!execute(true, retry)) return null;
		try {
			return fetchResultSet(statement.getResultSet());
		} catch (Exception e) {
			return null;
		} finally {
			try {
				statement.close();
				statement = null;
			} catch (SQLException e) {}
		}
    }
	
    public Map<String, Object> one() {
    	Integer offset = this.offset;
    	Integer limit = this.offset;
    	this.offset = null;
    	this.limit = 1;
    	sql = build();
    	List<Map<String, Object>> list = all();
    	this.offset = offset;
    	this.limit = limit;
    	if(null == list) return null;
    	if(list.isEmpty()) return new LinkedHashMap<>();
    	return list.get(0);
    }
 
    public Number calculate(String type, String field) {
    	String fields = select;
    	select = type + "(" + field + ") as calculate";
    	Map<String, Object> map = one();
    	select = fields;
    	if(null == map) return null;
    	return (Number) map.get("calculate");
    }
    
    public Number count() {
    	return count("*");
    }
    
    public Number count(String field) {
    	return calculate("COUNT", field);
    }
    
    public Number sum(String field) {
    	return calculate("SUM", field);
    }
    
    public Number average(String field) {
    	return calculate("AVG", field);
    }
    
    public Number min(String field) {
    	return calculate("MIN", field);
    }
    
    public Number max(String field) {
    	return calculate("MAX", field);
    }
    
    /**
     * 返回查询的条件是否存在
     */
    public boolean exists() {
        return count().intValue() > 0;
    }
    

//    
//    /**
//     * 返回多行单值
//     */
//    public function column() {
//        return $this->all(PDO::FETCH_COLUMN);
//    }
//    

//    
//    /**
//     * 单条插入
//     * @param array $data 插入的数据是以表字段名为下标的数组，如：['name' => 'Sam','age' => 30]
//     * @param type $needUpdate
//     */
//    public function insert($data, $needUpdate = false) {
//        $params = [];
//        $names = [];
//        $placeholders = [];
//        $this->prepareData($data);
//        foreach ($data as $name => $value) {
//            $names[] = $this->quoteColumnName($name);
//            $phName = self::PARAM_PREFIX . count($params);
//            $placeholders[] = $phName;
//            $params[$phName] = $value;
//        }
//        $this->sql = 'INSERT INTO ' . $this->tableName()
//        . (!empty($names) ? ' (' . implode(', ', $names) . ')' : '')
//        . (!empty($placeholders) ? ' VALUES (' . implode(', ', $placeholders) . ')' : ' DEFAULT VALUES');
//        if ($needUpdate) {
//            $this->sql .= $this->duplicateUpdate($names);
//        }
//        $this->bindValues($params);
//        $ret = $this->execute(false, self::$retry);
//        if ($ret) {
//            return $this->resource->lastInsertId();
//        } else if ($this->errorCode == '42S02' && $this->createTable()) {
//            return $this->insert($data, $needUpdate);
//        } else {
//            return null;
//        }
//    }
//    
//    /**
//     * 批量插入
//     * @param array $datas 插入的数据是以表字段名为下标的二维数组，如：[['name' => 'Sam1','age' => 30],['name' => 'Sam1','age' => 40]]
//     * @param boolean $needUpdate
//     */
//    public function batchInsert($datas, $needUpdate = false) {
//        if (!is_array($datas) || !is_array(current($datas))) {
//            return null;
//        }
//        $values = [];
//        $columns = null;
//        foreach ($datas as $row) {
//            $this->prepareData($row);
//            $vs = [];
//            !isset($columns) && $columns = array_keys($row);
//            foreach ($row as $i => $value) {
//                if (is_string($value)) {
//                    $value = $this->quoteValue($value);
//                } elseif ($value === false) {
//                    $value = 0;
//                } elseif ($value === null) {
//                    $value = "''";
//                }
//                $vs[] = $value;
//            }
//            $values[] = '(' . implode(', ', $vs) . ')';
//        }
//        foreach ($columns as $i => $name) {
//            $columns[$i] = $this->quoteColumnName($name);
//        }
//        $this->sql = 'INSERT INTO ' . $this->tableName() . ' (' . implode(', ', $columns) . ') VALUES ' . implode(', ', $values);
//        if ($needUpdate) {
//            $this->sql .= $this->duplicateUpdate($columns);
//        }
//        $ret = $this->execute(false, self::$retry);
//        if ($ret) {
//            return $this->statement->rowCount();
//        } else if ($this->errorCode == '42S02' && $this->createTable()) {
//            return $this->batchInsert($datas, $needUpdate);
//        } else {
//            return null;
//        }
//    }
//    
//    /**
//     * 更新并返回更新的行数
//     * @param arrya $data 数组数据 ['name' => 'Sam1','age' => 30]
//     * @param string|array $condition 参见 where()
//     * @param array $params 索引参数占位符的查询参数数组
//     */
//    public function update($data, $condition = '', $params = []) {
//        $lines = [];
//        $this->prepareData($data);
//        foreach ($data as $name => $value) {
//            $phName = self::PARAM_PREFIX . count($params);
//            $lines[] = $this->quoteColumnName($name) . '=' . $phName;
//            $params[$phName] = $value;
//        }
//        $sql = 'UPDATE ' . $this->tableName() . ' SET ' . implode(', ', $lines);
//        $where = $this->buildWhere($condition, $params);
//        $this->sql = $where === '' ? $sql : $sql . ' ' . $where;
//        $this->bindValues($params);
//        $ret = $this->execute(false, self::$retry);
//        if ($ret) {
//            return $this->statement->rowCount();
//        } else {
//            return null;
//        }
//    }
//    
//    /**
//     * 删除并返回删除的行数
//     * @param string|array $condition 参见 where()
//     * @param array $params 索引参数占位符的查询参数数组
//     */
//    public function delete($condition = '', $params = []) {
//        $sql = 'DELETE FROM ' . $this->tableName();
//        $where = $this->buildWhere($condition, $params);
//        $this->sql = $where === '' ? $sql : $sql . ' ' . $where;
//        $this->bindValues($params);
//        $ret = $this->execute(false, self::$retry);
//        if ($ret) {
//            return $this->statement->rowCount();
//        } else {
//            return null;
//        }
//    }
//    
//    /**
//     * 执行一条 SQL 语句，insert|update|delete返回受影响的行数,select返回PDOStatement对象,失败的情况会返回 null
//     * 不建议直接使用，需要自己处理参数安全转义
//     */
//    public function execSql($sql) {
//        try {
//            $forRead = $this->isReadQuery($sql);
//            if (!$this->isMaster && $forRead) {
//                $resource = $this->connection->getSlave();
//            } else {
//                $resource = $this->connection->getMaster();
//            }
//            if ($this->logger->isDebugEnabled()) {
//                $startTime = microtime(true);
//                $this->logger->debug("MySQL: sql = {$sql}");
//            }
//            $result = $forRead ? ($resource->query($sql)) : ($resource->exec($sql));
//            if ($this->logger->isDebugEnabled()) {
//                $cxcuteTime = microtime(true) - $startTime;
//                $this->logger->debug('<font color=' . ($cxcuteTime > 1 ? 'red' : 'green') . '>ExcuteTime: ' . $cxcuteTime . "</font>");
//            }
//            return $result;
//        } catch (PDOException $e) {
//            if ($this->logger->isDebugEnabled()) {
//                $this->logger->debug('<b style="color:red;">'.$e->getMessage()."</b>");
//            }
//            $this->errorCode = $e->getCode();
//            $this->errorMsg = $e->getMessage();
//            if ($this->connection->isTransaction()) {
//                throw $e;
//            }
//            return null;
//        }
//    }
//    
//    /**
//     * 清除 select() where() limit() offset() orderBy() groupBy() join() having()
//     */
//    public function reset() {
//        $this->statement = null;
//        $this->_pendingParams = [];
//        $this->select = null;
//        $this->where = null;
//        $this->limit = null;
//        $this->offset = null;
//        $this->orderBy = null;
//        $this->groupBy = null;
//        $this->join = null;
//        $this->having = null;
//        return $this;
//    }
//    
//    /**
//     * 清除已绑定的查询参数数组
//     */
//    public function cancelBindValues() {
//        $this->_pendingParams = [];
//        return $this;
//    }
//    
//    public function beginTransaction() {
//        $this->connection->beginTransaction();
//    }
//    
//    public function commit() {
//        $this->connection->commit();
//    }
//    
//    public function rollback() {
//        $this->connection->rollback();
//    }
//    
//    /**
//     * 要完成自动建表时，子类需要复写该方法
//     * @return boolean 建表成功需要返回true 否则返回false
//     */
//    protected function createTable() {
//        return false;
//    }


//    


//    private function duplicateUpdate($fields) {
//        $sql = " ON DUPLICATE KEY UPDATE ";
//        foreach ($fields as $field) {
//            $sql .= $field . ' = VALUES(' . $field . '),';
//        }
//        return substr($sql, 0, -1);
//    }
//    
//    private function quoteTableName($name) {
//        if (strpos($name, '(') !== false || strpos($name, '{{') !== false) {
//            return $name;
//        }
//        if (strpos($name, '.') === false) {
//            return $this->quoteSimpleTableName($name);
//        }
//        $parts = explode('.', $name);
//        foreach ($parts as $i => $part) {
//            $parts[$i] = $this->quoteSimpleTableName($part);
//        }
//        return implode('.', $parts);
//    }
//    
//    private function quoteSimpleTableName($name) {
//        return strpos($name, '`') !== false ? $name : "`$name`";
//    }
//    
//    private function quoteSimpleColumnName($name) {
//        return strpos($name, '`') !== false || $name === '*' ? $name : "`$name`";
//    }
//    
//    private function quoteValue($str) {
//        if (!is_string($str)) {
//            return $str;
//        }
//        if (!$this->resource) {
//            $this->resource = $this->connection->getSlave();
//        }
//        if (($value = $this->resource->quote($str)) !== false) {
//            return $value;
//        } else {
//            // the driver doesn't support quote (e.g. oci)
//            return "'" . addcslashes(str_replace("'", "''", $str), "\000\n\r\\\032") . "'";
//        }
//    }
//    
//    private function quoteColumnName($name) {
//        if (strpos($name, '(') !== false || strpos($name, '[[') !== false || strpos($name, '{{') !== false) {
//            return $name;
//        }
//        if (($pos = strrpos($name, '.')) !== false) {
//            $prefix = $this->quoteTableName(substr($name, 0, $pos)) . '.';
//            $name = substr($name, $pos + 1);
//        } else {
//            $prefix = '';
//        }
//        return $prefix . $this->quoteSimpleColumnName($name);
//    }
//    
//    private function isReadQuery($sql) {
//        $pattern = '/^\s*(SELECT|SHOW|DESCRIBE)\b/i';
//        return preg_match($pattern, $sql) > 0;
//    }
//    
//    private function getPdoType($data) {
//        static $typeMap = [
//            // php type => PDO type
//            'boolean' => PDO::PARAM_BOOL,
//            'integer' => PDO::PARAM_INT,
//            'string' => PDO::PARAM_STR,
//            'resource' => PDO::PARAM_LOB,
//            'NULL' => PDO::PARAM_NULL,
//        ];
//        $type = gettype($data);
//        return isset($typeMap[$type]) ? $typeMap[$type] : PDO::PARAM_STR;
//    }
}
