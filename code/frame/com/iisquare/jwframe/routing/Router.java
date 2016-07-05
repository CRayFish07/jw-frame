package com.iisquare.jwframe.routing;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import com.iisquare.jwframe.utils.DPUtil;

public class Router {
	
	private static ArrayList<RouteURI> routes = new ArrayList<>();
    private static LinkedHashMap<String, String> domains = new LinkedHashMap<>();
    
    static {
    	domains.put("*", "frontend");
    }
    
    public synchronized static void init(LinkedHashMap<String, String> customDomains) {
    	domains = new LinkedHashMap<>(); // 防止并发读写
    	for (Entry<String, String> entry : customDomains.entrySet()) {
    		String domain = "^" + entry.getKey().replaceAll("\\.", "\\\\.").replaceAll("\\*", ".+") + "$";
    		domains.put(domain, entry.getValue());
    	}
    }
    
    public static boolean get(String uri, Generator action) {
        return addRoute(new String[] {"GET", "HEAD"}, uri, action);
    }
    
    public static boolean post(String uri, Generator action) {
        return addRoute(new String[] {"POST"}, uri, action);
    }
    
    public static boolean put(String uri, Generator action) {
        return addRoute(new String[] {"PUT"}, uri, action);
    }
    
    public static boolean patch(String uri, Generator action) {
        return addRoute(new String[] {"PATCH"}, uri, action);
    }
    
    public static boolean delete(String uri, Generator action) {
        return addRoute(new String[] {"DELETE"}, uri, action);
    }
    
    public static boolean options(String uri, Generator action) {
        return addRoute(new String[] {"OPTIONS"}, uri, action);
    }
    
    public static boolean any(String uri, Generator action) {
        String[] verbs = new String[] {"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"};
        return addRoute(verbs, uri, action);
    }
    
    public static boolean match(String[] methods, String uri, Generator action) {
        return addRoute(methods, uri, action);
    }
    
    protected static boolean addRoute(String[] methods, String uri, Generator action) {
    	return routes.add(new RouteURI(methods, uri, action));
    }
    
    public String parseModule(String host) {
    	for (Entry<String, String> entry : domains.entrySet()) {
    		if(host.matches(entry.getKey())) return entry.getValue();
    	}
        return null;
    }
    
    public RouteAction parseRoute(String uri) {
        for (RouteURI route : routes) {
        	List<String> matches = DPUtil.getMatcher(route.getUri(), uri, true);
            if(matches.isEmpty()) continue ;
            Generator generator = route.getAction();
            if(null == generator) return null;
            return generator.call(DPUtil.collectionToStringArray(matches.subList(1, matches.size())));
        }
        return null;
    }
    /*
    public function conventionRoute(uri, config) {
        route = self::generateRoute(config["defaultControllerName"], config["defaultActionName"]);
        uri = trim(uri, "/");
        if(empty(uri)) return route;
        uriArray = explode("/", uri);
        length = count(uriArray);
        route["controller"] = uriArray[0];
        if(1 == length) return route;
        route["action"] = uriArray[1];
        if(2 == length) return route;
        if(empty(config["allowPathParams"]) || length % 2 != 0) return null;
        for (i = 2; i < length; i += 2) {
            route["params"][uriArray[i]] = uriArray[i + 1];
        }
        return route;
    }
    
    private function invoke(Application app, module, route, args = null) {
        config = app->getApplicationConfig();
        webApplicationContext = WebApplicationContext::getInstance();
        className = app->getRootNamespace().app->getApplicationDirectory()."\\".module."\\controller"
            ."\\".ucfirst(route["controller"]).config["defaultControllerSuffix"];
        instance = webApplicationContext->getBean(className);
        try {
            controller = new ReflectionClass(instance);
            instance->setAppUrl(app->getAppUrl());
            instance->setAppUri(app->getAppUri());
            instance->setAppPath(app->getAppPath());
            instance->setRootPath(app->getRootPath());
            instance->setModuleName(module);
            instance->setControllerName(route["controller"]);
            instance->setActionName(route["action"]);
            if(!is_array(route["params"])) route["params"] = [];
            route["params"] = array_merge(_REQUEST, route["params"]);
            instance->setParams(route["params"]);
            instance->setAssign([]);
            initVal = instance->init();
            if (null !== initVal) return new ApplicationException("initError");
            action = controller->getMethod(route["action"].config["defaultActionSuffix"]);
            if (null === args) {
                actionVal = action->invoke(instance);
            } else {
                actionVal = action->invoke(instance, args);
            }
            destroyVal = instance->destroy(actionVal);
            if (null !== destroyVal) return new ApplicationException("destroyError");
        } catch (Exception e) {
            return new Exception("Route:controller[".route["controller"]."] - action[".route["action"]."]", null, e);
        }
        return null;
    }
    
    public function dispatch(Application app) {
        config = app->getApplicationConfig();
        // 模块检测
        host = explode(":", _SERVER["HTTP_HOST"])[0];
        module = this->parseModule(host);
        if(null == module) return new ApplicationException("no module matches!");
        // 载入模块初始化文件
        filename = app->getRootPath().app->getApplicationDirectory()
            .DIRECTORY_SEPARATOR.module.DIRECTORY_SEPARATOR."init.php";
        if(file_exists(filename)) include_once filename;
        // URI检测
        uri = _SERVER["REQUEST_URI"];
        if(0 !== strpos(uri, app->getAppUri())) return new ApplicationException("app uri error!");
        uri = "/".substr(uri, strlen(app->getAppUri()));
        // 自定义路由检测
        route = this->parseRoute(uri);
        // 约定路由检测
        if(null == route) route = this->conventionRoute(uri, config);
        // 执行路由
        retVal = this->invoke(app, module, route);
        if(null === retVal) return null;
        // 执行路由失败，调用错误处理
        route = self::generateRoute(config["defaultErrorController"], config["defaultErrorAction"]);
        return this->invoke(app, module, route, retVal);
    }*/
}
