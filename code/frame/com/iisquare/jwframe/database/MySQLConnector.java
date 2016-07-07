package com.iisquare.jwframe.database;

/**
 * MySQL连接管理类，实例和单次请求绑定
 * @author Ouyang
 *
 */
public class MySQLConnector extends Connector {

	
	
	/*private $username;
    private $password;
    private $dbname;
    private $charset;
    private $tablePrefix;
    private $masterHost;
    private $masterProt;
    private $slaveHost;
    private $slavePort;
    private $options = null;
    private $masterResource;
    private $slaveResource;
    private $transactionLevel = 0;
    private static $connectionArray = array();
    
    public function getTablePrefix() {
        return $this->tablePrefix;
    }

    protected function __construct($config) {
        parent::__construct();
        $this->username = $config['username'];
        $this->password = $config['password'];
        $this->dbname = $config['dbname'];
        $this->charset = $config['charset'];
        $this->tablePrefix = $config['tablePrefix'];
        $master = $config['master'];
        $this->masterHost = $master['host'];
        $this->masterProt = $master['port'];
        if(empty($config['slaves'])) {
            $slave = $config['master'];
        } else {
            $slave = $config['slaves'][array_rand($config['slaves'])];
        }
        $this->slaveHost = $slave['host'];
        $this->slavePort = $slave['port'];
        if(isset($config['options'])) $this->options = $config['options'];
    }
    
    public static function getInstance($dbName = null, $charset = null) {
        $config = self::loadConfig('mysql');
        if(!empty($dbName)) $config['dbname'] = $dbName;
        if(!empty($charset)) $config['charset'] = $dbName;
        $key = '___'.$config['dbname'].'___'.$config['charset'].'___';
        if(!isset(self::$connectionArray[$key])) {
            self::$connectionArray[$key] = new self($config);
        }
        return self::$connectionArray[$key];
    }
    
    private function getResource($isMaster) {
        if ($isMaster) {
            $dsn = "mysql:host={$this->masterHost};port={$this->masterProt};dbname={$this->dbname}";
        } else {
            $dsn = "mysql:host={$this->slaveHost};port={$this->slavePort};dbname={$this->dbname}";
        }
        if (!empty($this->charset)) $dsn .= ';charset='.$this->charset;
        if(empty($this->options)) {
            $options = [
                PDO::ATTR_EMULATE_PREPARES => false,
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
            ];
        } else {
            $options = $this->options;
        }
        try {
            return new PDO($dsn, $this->username, $this->password, $options);
        } catch (PDOException $e) {
            if($this->logger->isErrorEnabled()) $this->logger->error($e->getMessage());
            return null;
        }
    }
    
    public function close() {
        if ($this->masterResource !== null) {
            $this->masterResource = null;
        }
        if ($this->slaveResource !== null) {
            $this->slaveResource = null;
        }
    }
    
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
