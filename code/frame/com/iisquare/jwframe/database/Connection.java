package com.iisquare.jwframe.database;

public abstract class Connection {
	
	/*private static $config;
    protected $logger;
    
    protected function __construct() {
        $this->logger = Logger::getInstance();
    }
    
    protected static function loadConfig($dbType) {
        if(null == self::$config) {
            *//**
             * @var Application $app
             *//*
            $app = WebApplicationContext::getInstance()->getBean(Application::class);
            self::$config = require_once $app->getRootPath().$app->getConfigDirectory().DIRECTORY_SEPARATOR.'db.config.php';
        }
        if(null == $dbType) return self::$config;
        if(isset(self::$config[$dbType])) return self::$config[$dbType];
        return null;
    }*/

}
