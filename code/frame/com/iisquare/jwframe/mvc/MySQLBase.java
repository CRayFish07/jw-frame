package com.iisquare.jwframe.mvc;

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
	private int timeout = 250; // 获取连接超时时间
	private Object where;
	private Integer limit;
	private Integer offset;
	private Object orderBy;
	private List<String> select;
	private String selectOption;
	private boolean distinct = false;
	private Object groupBy;
	private Object join;
	private Object having;
	private Object resource; // 当前连接资源
	private Object statement; // 当前预处理对象
	private Object _pendingParams;
	private String sql;
	private Exception exception;
	private boolean isMaster = false;
	
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
		this.timeout = timeout;
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
    
    /**
     * 设置额外的查询字段选项
     * @param selectOption
     */
	public void setSelectOption(String selectOption) {
		this.selectOption = selectOption;
	}

	/**
	 * 查询返回字段
	 * @param columns String or List<String>
	 */
    @SuppressWarnings("unchecked")
	public MySQLBase select(Object columns) {
    	List<String> list;
    	if(columns instanceof List) {
    		list = (List<String>) columns;
    	} else {
    		list = DPUtil.stringArrayToList(DPUtil.explode(DPUtil.parseString(columns), "\\s*,\\s*", " ", true));
    	}
        select = list;
        return this;
    }
    
    
    /**
     * @see select()
     */
    @SuppressWarnings("unchecked")
	public MySQLBase addSelect(Object columns) {
    	List<String> list;
    	if(columns instanceof List) {
    		list = (List<String>) columns;
    	} else {
    		list = DPUtil.stringArrayToList(DPUtil.explode(DPUtil.parseString(columns), "\\s*,\\s*", " ", true));
    	}
        if (null == select) {
        	select = list;
        } else {
        	for (String item : list) {
        		select.add(item);
        	}
        }
        return this;
    }
    
    /**
     * 去除重复行
     */
    public MySQLBase distinct(boolean value) {
        distinct = value;
        return this;
    }
    
//    /**
//     * 方法定义了 SQL 语句当中的 WHERE 子句。 你可以使用如下三种格式来定义 WHERE 条件：
//     *
//     * 字符串格式，例如：'status=1'
//     * 哈希格式，例如： ['column1' => value1, 'column2' => value2, ...]
//     * 操作符格式，例如：[operator,operand1, operand2, ...]
//     *
//     * 字符串格式
//     * 在定义非常简单的查询条件的时候，字符串格式是最合适的。它看起来和原生 SQL 语句差不多。
//     * - $query->where('status=1');
//     * 或者使用参数绑定来绑定动态参数值
//     * - $query->where('status=:status', [':status' => $status]);
//     * {{{危险！}}} 千万别这样干，除非你非常的确定 $status 是一个可信任值。
//     * - $query->where("status=$status");
//     *
//     * 哈希格式
//     * 哈希格式最适合用来指定多个 AND 串联起来的简单的"等于查询"。 它是以数组的形式来书写的，数组的键表示字段的名称，而数组的值则表示 这个字段需要匹配的值。
//     * - ['type' => 1, 'status' => 2] 将会生成 (type = 1) AND (status = 2).
//     * - ['id' => [1, 2, 3], 'status' => 2] 将会生成 (id IN (1, 2, 3)) AND (status = 2).
//     * - ['status' => null] 将会生成 status IS NULL.
//     *
//     * 操作符格式
//     * [操作符, 操作数1, 操作数2, ...] 其中每个操作数可以是字符串格式、哈希格式或者嵌套的操作符格式，而操作符可以是如下列表中的一个：
//     * - **and**: 操作数会被 AND 关键字串联起来。
//     *   ['and', 'id=1', 'id=2'] 将会生成 id=1 AND id=2。
//     *   如果操作数是一个数组，它也会按上述规则转换成 字符串。
//     *   ['and', 'type=1', ['or', 'id=1', 'id=2']] 将会生成 type=1 AND (id=1 OR id=2)。
//     *   这个方法不会自动加引号或者转义。
//     * - **or**: 用法和 and 操作符类似:
//     *   ['or', ['type' => [7, 8, 9]], ['id' => [1, 2, 3]] will 将会生成 (type IN (7, 8, 9) OR (id IN (1, 2, 3))).
//     * - **not**: 否定后面的操作数.
//     *   ['not', ['attribute' => null]] 将会生成 NOT (attribute IS NULL).
//     * - **between**: 第一个操作数为字段名称，第二个和第三个操作数代表的是这个字段 的取值范围
//     *   ['between', 'id', 1, 10] 将会生成 id BETWEEN 1 AND 10.
//     * - **not between**: 用法和 BETWEEN 操作符类似
//     * - **in**: 第一个操作数应为字段名称。第二个操作符是一个数组。
//     *   ['in', 'id', [1, 2, 3]] 将会生成 id IN (1, 2, 3).
//     *   创建一个组合条件，可以使用数组为列名称和值，其中的值是列名称的索引
//     *   ['in', ['id', 'name'], [['id' => 1, 'name' => 'foo'], ['id' => 2, 'name' => 'bar']] ].
//     * - **not in**: 用法和 in 操作符类似
//     * - **like**: 第一个操作数应为一个字段名，第二个操作数可以使字符串或数组， 代表第一个操作数需要模糊查询的值。
//     *   ['like', 'name', 'tester'] 会生成 name LIKE '%tester%'。
//     *   如果范围值是一个数组，那么将会生成用 AND 串联起来的 多个 like 语句。
//     *   ['like', 'name', ['test', 'sample']] 将会生成 name LIKE '%test%' AND name LIKE '%sample%'。
//     *   你也可以提供第三个可选的操作数来指定应该如何转义数值当中的特殊字符。
//     *   该操作数是一个从需要被转义的特殊字符到转义副本的数组映射。
//     *   如果没有提供这个操作数，将会使用默认的转义映射。如果需要禁用转义的功能， 只需要将参数设置为 false 或者传入一个空数组即可。
//     *   需要注意的是， 当使用转义映射（又或者没有提供第三个操作数的时候），第二个操作数的值的前后 将会被加上百分号。
//     *   ['like', 'name', '%tester', false]` will generate `name LIKE '%tester'
//     * - **or like**: 用法和 like 操作符类似，区别在于当第二个操作数为数组时， 会使用 OR 来串联多个 LIKE 条件语句。
//     * - **not like**: 用法和 like 操作符类似，区别在于会使用 NOT LIKE 来生成条件语句。
//     * - **or not like**: 用法和 not like 操作符类似，区别在于会使用 OR 来串联多个 NOT LIKE 条件语句。
//     * - 此外，您可以指定任意运算符，第一个操作数必须为字段的名称， 而第二个操作数则应为一个值。
//     *   ['>=', 'id', 10]` 将会生成 `id >= 10`.
//     *
//     * 你可以使用andWhere()或者orWhere() 在原有条件的基础上 附加额外的条件。你可以多次调用这些方法来分别追加不同的条件
//     */
//    public function where($condition, $params = []) {
//        $this->where = $condition;
//        return $this->bindValues($params);
//    }
//    
//    /**
//     * @see where()
//     */
//    public function andWhere($condition, $params = []) {
//        if ($this->where === null) {
//            $this->where = $condition;
//        } else {
//            $this->where = ['and', $this->where, $condition];
//        }
//        return $this->bindValues($params);
//    }
//    
//    /**
//     * @see where()
//     */
//    public function orWhere($condition, $params = []) {
//        if ($this->where === null) {
//            $this->where = $condition;
//        } else {
//            $this->where = ['or', $this->where, $condition];
//        }
//        return $this->bindValues($params);
//    }
//    
//    /**
//     * 连接查询
//     * @param string $table 将要连接的表名称。
//     * @param string|array $on 可选参数，连接条件，即 ON 子句。@see where()
//     * @param array $params 可选参数，与连接条件绑定的参数。
//     */
//    public function innerJoin($table, $on = '', $params = []) {
//        $this->join[] = ['INNER JOIN', $table, $on];
//        return $this->bindValues($params);
//    }
//    
//    /**
//     * @see innerJoin()
//     */
//    public function leftJoin($table, $on = '', $params = []) {
//        $this->join[] = ['LEFT JOIN', $table, $on];
//        return $this->bindValues($params);
//    }
//    
//    /**
//     * @see innerJoin()
//     */
//    public function rightJoin($table, $on = '', $params = []) {
//        $this->join[] = ['RIGHT JOIN', $table, $on];
//        return $this->bindValues($params);
//    }
//    
//    /**
//     * 数组形式：$query->groupBy(['id', 'status']); 或  $query->groupBy('id, status');
//     */
//    public function groupBy($columns) {
//        if (!is_array($columns)) {
//            $columns = preg_split('/\s*,\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
//        }
//        $this->groupBy = $columns;
//        return $this;
//    }
//    
//    /**
//     * 数组形式：$query->orderBy(['id' => SORT_ASC,'name' => SORT_DESC]); 或 $query->orderBy('id ASC, name DESC');
//     */
//    public function orderBy($columns) {
//        if (!is_array($columns)) {
//            $columns = preg_split('/\s*,\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
//            $result = [];
//            foreach ($columns as $column) {
//                if (preg_match('/^(.*?)\s+(asc|desc)$/i', $column, $matches)) {
//                    $result[$matches[1]] = strcasecmp($matches[2], 'desc') ? SORT_ASC : SORT_DESC;
//                } else {
//                    $result[$column] = SORT_ASC;
//                }
//            }
//            $this->orderBy = $result;
//        } else {
//            $this->orderBy = $columns;
//        }
//        return $this;
//    }
//    
//    /**
//     * 方法是用来指定 SQL 语句当中的 HAVING 子句。它带有一个条件， 和 where() 中指定条件的方法一样。
//     * @see where()
//     */
//    public function having($condition, $params = []) {
//        $this->having = $condition;
//        return $this->bindValues($params);
//    }
//    
//    public function limit($limit) {
//        $this->limit = $limit;
//        return $this;
//    }
//    
//    public function offset($offset) {
//        $this->offset = $offset;
//        return $this;
//    }
//    
//    /**
//     * 将值绑定到一个命名占位符参数
//     * @param string $name 命名占位符
//     * @param mixed $value 值
//     * @param integer $dataType SQL数据类型的参数。如果为空，类型是PHP的数据类型。
//     */
//    public function bindValue($name, $value, $dataType = null) {
//        if ($dataType === null) {
//            $dataType = $this->getPdoType($value);
//        }
//        $this->_pendingParams[$name] = [$value, $dataType];
//        return $this;
//    }
//    
//    /**
//     * 将多个值绑定到多个命名占位符参数，每个值的类型是PHP的数据类型
//     * [':name' => 'John', ':age' => 25]
//     */
//    public function bindValues($values) {
//        if (empty($values)) {
//            return $this;
//        }
//        foreach ($values as $name => $value) {
//            if (is_array($value)) {
//                $this->_pendingParams[$name] = $value;
//            } else {
//                $type = $this->getPdoType($value);
//                $this->_pendingParams[$name] = [$value, $type];
//            }
//        }
//        return $this;
//    }
//    
//    /**
//     * 准备一次预处理语句而执行多次查询。提示，在执行前绑定变量，然后在每个执行中改变变量的值（一般用在循环中）比较高效.
//     * @param string $name 命名占位符
//     * @param mixed $value 绑定到SQL语句参数PHP变量值
//     * @param integer $dataType SQL数据类型的参数。如果为空，类型是PHP的数据类型。
//     * @param integer $length 数据类型长度
//     * @param mixed $driverOptions 驱动程序特定选项
//     * @see http://www.php.net/manual/en/function.PDOStatement-bindParam.php
//     */
//    public function bindParam($name, &$value, $dataType = null, $length = null, $driverOptions = null) {
//        if (!$this->statement) {
//            throw new PDOException('bindParam must exist statement.call method insert update delete query..');
//        }
//        if ($dataType === null) {
//            $dataType = $this->getPdoType($value);
//        }
//        if ($length === null) {
//            $this->statement->bindParam($name, $value, $dataType);
//        } elseif ($driverOptions === null) {
//            $this->statement->bindParam($name, $value, $dataType, $length);
//        } else {
//            $this->statement->bindParam($name, $value, $dataType, $length, $driverOptions);
//        }
//        return $this;
//    }
//    
//    /**
//     * 返回 select count($q) 数量
//     */
//    public function count($q = '*') {
//        return $this->one("COUNT($q)");
//    }
//    
//    /**
//     * 返回 select sum($q) 总数
//     */
//    public function sum($q) {
//        return $this->one("SUM($q)");
//    }
//    
//    /**
//     * 返回 select avg($q) 均值
//     */
//    public function average($q) {
//        return $this->one("AVG($q)");
//    }
//    
//    /**
//     * 返回 select min($q) 最小值
//     */
//    public function min($q) {
//        return $this->one("MIN($q)");
//    }
//    
//    /**
//     * 返回 select max($q) 最大值
//     */
//    public function max($q) {
//        return $this->one("MAX($q)");
//    }
//    
//    /**
//     * 返回查询的条件是否存在
//     */
//    public function exists() {
//        return $this->count() > 0;
//    }
//    
//    /**
//     * 返回查询的数据资源对象，使用fetch()方法遍历数据，如果取出数据后要循环处理，建议使用该方法
//     */
//    public function query() {
//        $this->sql = $this->build();
//        if ($this->execute(true, self::$retry)) {
//            return $this->statement;
//        }
//        return null;
//    }
//    
//    /**
//     * 返回查询的所有数据数组
//     */
//    public function all($fetchMode = null) {
//        $this->sql = $this->build();
//        if ($this->execute(true, self::$retry)) {
//            if ($fetchMode === null) {
//                $fetchMode = PDO::FETCH_ASSOC;
//            }
//            $return = $this->statement->fetchAll($fetchMode);
//            $this->statement->closeCursor();
//            return $return;
//        }
//        return null;
//    }
//    
//    /**
//     * 返回多行单值
//     */
//    public function column() {
//        return $this->all(PDO::FETCH_COLUMN);
//    }
//    
//    /**
//     * 查询标量值/计算值
//     * @param string $field 可选参数 数据表中字段，如果设值，将返回该字段标量，否则按select()方法设值的字段返回一行
//     */
//    public function one($field = '') {
//        if (!empty($field)) {
//            $select = $this->select;
//            $this->select = [$field];
//        }
//        $limit = $this->limit;
//        $offset = $this->offset;
//        $this->limit = 1;
//        $this->offset = null;
//        $this->sql = $this->build();
//        $return = null;
//        if ($this->execute(true, self::$retry)) {
//            if (!empty($field)) {
//                $return = $this->statement->fetchColumn(0);
//            } else {
//                $return = $this->statement->fetch(PDO::FETCH_ASSOC);
//            }
//            $this->statement->closeCursor();
//        }
//        $this->limit = $limit;
//        $this->offset = $offset;
//        if (!empty($field)) {
//            $this->select = $select;
//        }
//        return $return;
//    }
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
//    private function execute($forRead, $curreconn = 0) {
//        if (!isset($this->sql) || empty($this->sql)) {
//            return null;
//        }
//        try {
//            if (!$this->statement || $this->statement->queryString != $this->sql) {
//                if ($this->statement) {
//                    $this->statement->closeCursor();
//                    $this->statement = null;
//                }
//                if ($this->isMaster || $this->connection->isTransaction()) {
//                    $forRead = false;
//                }
//                if ($forRead) {
//                    $this->resource = $this->connection->getSlave();
//                } else {
//                    $this->resource = $this->connection->getMaster();
//                }
//                if ($this->logger->isDebugEnabled()) {
//                    $this->logger->debug("Debug: sql = {$this->sql}");
//                    $startTime = microtime(true);
//                }
//                $this->statement = $this->resource->prepare($this->sql);
//                if ($this->logger->isDebugEnabled()) {
//                    $bindTime = microtime(true) - $startTime;
//                    $this->logger->debug('<font color="' . ($bindTime > 1 ? 'red' : 'green') . '">prepareTime: ' . $bindTime . "</font>");
//                }
//            }
//            $this->bindPendingParams();
//            if ($this->logger->isDebugEnabled()) {
//                $startTime = microtime(true);
//            }
//            $result = $this->statement->execute();
//            if ($this->logger->isDebugEnabled()) {
//                $cxcuteTime = microtime(true) - $startTime;
//                $this->logger->debug('<font color="' . ($cxcuteTime > 1 ? 'red' : 'green') . '">ExcuteTime: ' . $cxcuteTime . "</font>");
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
//            if ($curreconn > 0 && $e->errorInfo[1] == 2006) {
//                if ($this->logger->isDebugEnabled()) {
//                    $this->logger->debug('<font color=red>reconn:' . $curreconn . '</font>');
//                }
//                $curreconn--;
//                $this->connection->close();
//                return $this->execute($forRead, $curreconn);
//            }
//            return null;
//        }
//    }
//    
//    private function bindPendingParams() {
//        if ($this->logger->isTraceEnabled()) {
//            foreach ($this->_pendingParams as $k => $v) {
//                $this->logger->trace($k.'=>'.$v[0]);
//            }
//        }
//        foreach ($this->_pendingParams as $name => $value) {
//            $this->statement->bindValue($name, $value[0], $value[1]);
//        }
//        $this->_pendingParams = [];
//    }
//    
//    private function build() {
//        $params = [];
//        $clauses = [
//            $this->buildSelect($this->select, $this->distinct, $this->selectOption),
//            'FROM ' . $this->tableName(),
//            $this->buildJoin($this->join, $params),
//            $this->buildWhere($this->where, $params),
//            $this->buildGroupBy($this->groupBy),
//            $this->buildHaving($this->having, $params),
//        ];
//        $sql = implode(' ', array_filter($clauses));
//        $sql = $this->buildOrderByAndLimit($sql, $this->orderBy, $this->limit, $this->offset);
//        $this->bindValues($params);
//        return $sql;
//    }
//    
//    private function buildSelect($columns, $distinct = false, $selectOption = null) {
//        $select = $distinct ? 'SELECT DISTINCT' : 'SELECT';
//        if ($selectOption !== null) {
//            $select .= ' ' . $selectOption;
//        }
//        if (empty($columns)) {
//            return $select . ' *';
//        }
//        foreach ($columns as $i => $column) {
//            if (is_string($i)) {
//                if (strpos($column, '(') === false) {
//                    $column = $this->quoteColumnName($column);
//                }
//                $columns[$i] = "$column AS " . $this->quoteColumnName($i);
//            } elseif (strpos($column, '(') === false) {
//                $columns[$i] = $this->quoteColumnName($column);
//            }
//        }
//        return $select . ' ' . implode(', ', $columns);
//    }
//    
//    private function buildJoin($joins, &$params) {
//        if (empty($joins)) {
//            return '';
//        }
//        foreach ($joins as $i => $join) {
//            if (!is_array($join) || !isset($join[0], $join[1])) {
//                throw new PDOException('A join clause must be specified as an array of join type, join table, and optionally join condition.');
//            }
//            // 0:join type, 1:join table, 2:on-condition (optional)
//            list ($joinType, $table) = $join;
//            $joins[$i] = "$joinType $table";
//            if (isset($join[2])) {
//                $condition = $this->buildCondition($join[2], $params);
//                if ($condition !== '') {
//                    $joins[$i] .= ' ON ' . $condition;
//                }
//            }
//        }
//    
//        return implode(' ', $joins);
//    }
//    
//    private function buildWhere($condition, &$params) {
//        $where = $this->buildCondition($condition, $params);
//        return $where === '' ? '' : 'WHERE ' . $where;
//    }
//    
//    private function buildGroupBy($columns) {
//        if (empty($columns)) {
//            return '';
//        }
//        foreach ($columns as $i => $column) {
//            $columns[$i] = $this->quoteColumnName($column);
//        }
//        return 'GROUP BY ' . implode(',', $columns);
//    }
//    
//    private function buildHaving($condition, &$params) {
//        $having = $this->buildCondition($condition, $params);
//        return $having === '' ? '' : 'HAVING ' . $having;
//    }
//    
//    private function buildOrderByAndLimit($sql, $orderBy, $limit, $offset) {
//        $orderBy = $this->buildOrderBy($orderBy);
//        if ($orderBy !== '') {
//            $sql .= ' ' . $orderBy;
//        }
//        $limit = $this->buildLimit($limit, $offset);
//        if ($limit !== '') {
//            $sql .= ' ' . $limit;
//        }
//        return $sql;
//    }
//    
//    private function buildOrderBy($columns) {
//        if (empty($columns)) {
//            return '';
//        }
//        $orders = [];
//        foreach ($columns as $name => $direction) {
//            $orders[] = $this->quoteColumnName($name) . ($direction === SORT_DESC ? ' DESC' : '');
//        }
//        return 'ORDER BY ' . implode(', ', $orders);
//    }
//    
//    private function buildLimit($limit, $offset) {
//        $sql = '';
//        if (!empty($offset)) {
//            $sql .= $offset . ',';
//        }
//        if (!empty($limit)) {
//            $sql = $limit;
//        }
//        return empty($sql) ? '' : 'LIMIT ' . $sql;
//    }
//    
//    private function buildCondition($condition, &$params) {
//        if (!is_array($condition)) {
//            return (string) $condition;
//        } elseif (empty($condition)) {
//            return '';
//        }
//        if (isset($condition[0])) { // operator format: operator, operand 1, operand 2, ...
//            $operator = strtoupper($condition[0]);
//            array_shift($condition);
//            switch ($operator) {
//                case 'NOT':
//                    return $this->buildNotCondition($operator, $condition, $params);
//                case 'AND':
//                case 'OR':
//                    return $this->buildAndCondition($operator, $condition, $params);
//                case 'BETWEEN':
//                case 'NOT BETWEEN':
//                    return $this->buildBetweenCondition($operator, $condition, $params);
//                case 'IN':
//                case 'NOT IN':
//                    return $this->buildInCondition($operator, $condition, $params);
//                case 'LIKE':
//                case 'NOT LIKE':
//                case 'OR LIKE':
//                case 'OR NOT LIKE':
//                    return $this->buildLikeCondition($operator, $condition, $params);
//                default :
//                    return $this->buildSimpleCondition($operator, $condition, $params);
//            }
//        } else { // hash format: 'column1' => 'value1', 'column2' => 'value2', ...
//            return $this->buildHashCondition($condition, $params);
//        }
//    }
//    
//    private function buildHashCondition($condition, &$params) {
//        $parts = [];
//        foreach ($condition as $column => $value) {
//            if (strpos($column, '(') === false) {
//                $column = $this->quoteColumnName($column);
//            }
//            if ($value === null) {
//                $parts[] = "$column IS NULL";
//            } else {
//                $phName = self::PARAM_PREFIX . count($params);
//                $parts[] = "$column=$phName";
//                $params[$phName] = $value;
//            }
//        }
//        return count($parts) === 1 ? $parts[0] : '(' . implode(') AND (', $parts) . ')';
//    }
//    
//    private function buildAndCondition($operator, $operands, &$params) {
//        $parts = [];
//        foreach ($operands as $operand) {
//            if (is_array($operand)) {
//                $operand = $this->buildCondition($operand, $params);
//            }
//            if ($operand !== '') {
//                $parts[] = $operand;
//            }
//        }
//        if (!empty($parts)) {
//            return '(' . implode(") $operator (", $parts) . ')';
//        } else {
//            return '';
//        }
//    }
//    
//    private function buildNotCondition($operator, $operands, &$params) {
//        if (count($operands) !== 1) {
//            throw new PDOException("Operator '$operator' requires exactly one operand.");
//        }
//        $operand = reset($operands);
//        if (is_array($operand)) {
//            $operand = $this->buildCondition($operand, $params);
//        }
//        if ($operand === '') {
//            return '';
//        }
//        return "$operator ($operand)";
//    }
//    
//    private function buildBetweenCondition($operator, $operands, &$params) {
//        if (!isset($operands[0], $operands[1], $operands[2])) {
//            throw new PDOException("Operator '$operator' requires three operands.");
//        }
//        list($column, $value1, $value2) = $operands;
//        if (strpos($column, '(') === false) {
//            $column = $this->quoteColumnName($column);
//        }
//        $phName1 = self::PARAM_PREFIX . count($params);
//        $params[$phName1] = $value1;
//        $phName2 = self::PARAM_PREFIX . count($params);
//        $params[$phName2] = $value2;
//        return "$column $operator $phName1 AND $phName2";
//    }
//    
//    private function buildInCondition($operator, $operands, &$params) {
//        if (!isset($operands[0], $operands[1])) {
//            throw new PDOException("Operator '$operator' requires two operands.");
//        }
//        list($column, $values) = $operands;
//        if ($values === [] || $column === []) {
//            return $operator === 'IN' ? '0=1' : '';
//        }
//        $values = (array) $values;
//        if (count($column) > 1) {
//            return $this->buildCompositeInCondition($operator, $column, $values, $params);
//        }
//        if (is_array($column)) {
//            $column = reset($column);
//        }
//        foreach ($values as $i => $value) {
//            if (is_array($value)) {
//                $value = isset($value[$column]) ? $value[$column] : null;
//            }
//            if ($value === null) {
//                $values[$i] = 'NULL';
//            } else {
//                $phName = self::PARAM_PREFIX . count($params);
//                $params[$phName] = $value;
//                $values[$i] = $phName;
//            }
//        }
//        if (strpos($column, '(') === false) {
//            $column = $this->quoteColumnName($column);
//        }
//        if (count($values) > 1) {
//            return "$column $operator (" . implode(', ', $values) . ')';
//        } else {
//            $operator = $operator === 'IN' ? '=' : '<>';
//            return $column . $operator . reset($values);
//        }
//    }
//    
//    private function buildCompositeInCondition($operator, $columns, $values, &$params) {
//        $vss = [];
//        foreach ($values as $value) {
//            $vs = [];
//            foreach ($columns as $column) {
//                if (isset($value[$column])) {
//                    $phName = self::PARAM_PREFIX . count($params);
//                    $params[$phName] = $value[$column];
//                    $vs[] = $phName;
//                } else {
//                    $vs[] = 'NULL';
//                }
//            }
//            $vss[] = '(' . implode(', ', $vs) . ')';
//        }
//        foreach ($columns as $i => $column) {
//            if (strpos($column, '(') === false) {
//                $columns[$i] = $this->quoteColumnName($column);
//            }
//        }
//    
//        return '(' . implode(', ', $columns) . ") $operator (" . implode(', ', $vss) . ')';
//    }
//    
//    private function buildLikeCondition($operator, $operands, &$params) {
//        if (!isset($operands[0], $operands[1])) {
//            throw new PDOException("Operator '$operator' requires two operands.");
//        }
//        $escape = isset($operands[2]) ? $operands[2] : ['%' => '\%', '_' => '\_', '\\' => '\\\\'];
//        unset($operands[2]);
//        if (!preg_match('/^(AND |OR |)(((NOT |))I?LIKE)/', $operator, $matches)) {
//            throw new PDOException("Invalid operator '$operator'.");
//        }
//        $andor = ' ' . (!empty($matches[1]) ? $matches[1] : 'AND ');
//        $not = !empty($matches[3]);
//        $operator = $matches[2];
//        list($column, $values) = $operands;
//        if (!is_array($values)) {
//            $values = [$values];
//        }
//        if (empty($values)) {
//            return $not ? '' : '0=1';
//        }
//        if (strpos($column, '(') === false) {
//            $column = $this->quoteColumnName($column);
//        }
//        $parts = [];
//        foreach ($values as $value) {
//            $phName = self::PARAM_PREFIX . count($params);
//            $params[$phName] = empty($escape) ? $value : ('%' . strtr($value, $escape) . '%');
//            $parts[] = "$column $operator $phName";
//        }
//        return implode($andor, $parts);
//    }
//    
//    private function buildSimpleCondition($operator, $operands, &$params) {
//        if (count($operands) !== 2) {
//            throw new PDOException("Operator '$operator' requires two operands.");
//        }
//        list($column, $value) = $operands;
//        if (strpos($column, '(') === false) {
//            $column = $this->quoteColumnName($column);
//        }
//        if ($value === null) {
//            return "$column $operator NULL";
//        } else {
//            $phName = self::PARAM_PREFIX . count($params);
//            $params[$phName] = $value;
//            return "$column $operator $phName";
//        }
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
