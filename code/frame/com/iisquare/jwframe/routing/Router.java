package com.iisquare.jwframe.routing;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.iisquare.jwframe.Configuration;
import com.iisquare.jwframe.utils.DPUtil;

public class Router {
	
	private static Map<String, ArrayList<RouteURI>> routes = new Hashtable<>();
    private static LinkedHashMap<String, String> domains = new LinkedHashMap<>();
    private WebApplicationContext wac;
    private Configuration configuration;
    private String appUri, rootPath;
    private HttpServletRequest request;
	private HttpServletResponse response;
    
    static {
    	domains.put("*", "frontend");
    }
    
    public Router(String appUri, String rootPath, HttpServletRequest request,
			HttpServletResponse response) {
		this.appUri = appUri;
		this.rootPath = rootPath;
		this.request = request;
		this.response = response;
		wac = WebApplicationContextUtils.getRequiredWebApplicationContext(request.getServletContext());
		configuration = wac.getBean(Configuration.class);
	}

	public synchronized static void init(LinkedHashMap<String, String> customDomains) {
    	domains = new LinkedHashMap<>(); // 避免并发读写
    	for (Entry<String, String> entry : customDomains.entrySet()) {
    		String domain = "^" + entry.getKey().replaceAll("\\.", "\\\\.").replaceAll("\\*", ".+") + "$";
    		domains.put(domain, entry.getValue());
    	}
    }
    
    public static boolean get(String module, String uri, Generator action) {
        return addRoute(module, new String[] {"GET", "HEAD"}, uri, action);
    }
    
    public static boolean post(String module, String uri, Generator action) {
        return addRoute(module, new String[] {"POST"}, uri, action);
    }
    
    public static boolean put(String module, String uri, Generator action) {
        return addRoute(module, new String[] {"PUT"}, uri, action);
    }
    
    public static boolean patch(String module, String uri, Generator action) {
        return addRoute(module, new String[] {"PATCH"}, uri, action);
    }
    
    public static boolean delete(String module, String uri, Generator action) {
        return addRoute(module, new String[] {"DELETE"}, uri, action);
    }
    
    public static boolean options(String module, String uri, Generator action) {
        return addRoute(module, new String[] {"OPTIONS"}, uri, action);
    }
    
    public static boolean any(String module, String uri, Generator action) {
        String[] verbs = new String[] {"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"};
        return addRoute(module, verbs, uri, action);
    }
    
    public static boolean match(String module, String[] methods, String uri, Generator action) {
        return addRoute(module, methods, uri, action);
    }
    
    protected synchronized static boolean addRoute(String module, String[] methods, String uri, Generator action) {
    	ArrayList<RouteURI> list = routes.get(module);
    	if(null == list) {
    		list = new ArrayList<>();
    		routes.put(module, list);
    	}
    	return list.add(new RouteURI(methods, uri, action));
    }
    
    public String parseModule(String host) {
    	for (Entry<String, String> entry : domains.entrySet()) {
    		if(host.matches(entry.getKey())) return entry.getValue();
    	}
        return null;
    }
    
    public RouteAction parseRoute(String module, String uri) {
    	ArrayList<RouteURI> list = routes.get(module);
    	if(null == list) return null;
        for (RouteURI route : list) {
        	List<String> matches = DPUtil.getMatcher(route.getUri(), uri, true);
            if(matches.isEmpty()) continue ;
            Generator generator = route.getAction();
            if(null == generator) return null;
            return generator.call(DPUtil.collectionToStringArray(matches.subList(1, matches.size())));
        }
        return null;
    }
    
    public RouteAction conventionRoute(String uri) {
        RouteAction route = new RouteAction(
        		configuration.getDefaultControllerName(), configuration.getDefaultActionName(), null);
        uri = DPUtil.trim(uri, "/");
        if("".equals(uri)) return route;
        String[] uriArray = DPUtil.explode(uri, "/", null, false);
        route.setControllerName(uriArray[0]);
        if(1 == uriArray.length) return route;
        route.setActionName(uriArray[1]);
        if(2 == uriArray.length) return route;
        if(DPUtil.empty(configuration.getAllowPathParams()) || uriArray.length % 2 != 0) return null;
        Map<String, String[]> params = new LinkedHashMap<>();
        for (int i = 2; i < uriArray.length; i += 2) {
        	params.put(uriArray[i], new String[] {uriArray[i + 1]});
        }
        route.setParams(params);
        return route;
    }
    
    private Object invoke(String module, RouteAction route, Object arg) throws Exception {
    	String controllerName = route.getControllerName();
		String actionName = route.getActionName();
    	Class<?> controller = Class.forName(configuration.getControllerNamePath()
				+ "." + controllerName.substring(0, 1).toUpperCase()
				+ controllerName.substring(1)
				+ configuration.getDefaultControllerSuffix());
    	
    	return null;
        /*className = app->getRootNamespace().app->getApplicationDirectory()."\\".module."\\controller"
            ."\\".ucfirst(route["controller"]).configuration["defaultControllerSuffix"];
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
            action = controller->getMethod(route["action"].configuration["defaultActionSuffix"]);
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
        return null;*/
    }
    /*
    public function dispatch(Application app) {
        configuration = app->getApplicationConfig();
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
        if(null == route) route = this->conventionRoute(uri, configuration);
        // 执行路由
        retVal = this->invoke(app, module, route);
        if(null === retVal) return null;
        // 执行路由失败，调用错误处理
        route = self::generateRoute(configuration["defaultErrorController"], configuration["defaultErrorAction"]);
        return this->invoke(app, module, route, retVal);
    }*/
}
