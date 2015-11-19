package com.iisquare.jw.frame;

import java.util.Hashtable;
import java.util.Properties;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import com.iisquare.jw.frame.util.PropertiesUtil;

@Component
@Scope("application")
public class FrameConfigration {
	
	private Hashtable<String, String> configParams = null;
	
	public FrameConfigration() {
		configParams = new Hashtable<>();
		Properties prop = PropertiesUtil.load(this.getClass().getClassLoader(), "frame.properties");
		configParams.put("characterEncoding", prop.getProperty("characterEncoding", "UTF-8"));
		configParams.put("contentType", prop.getProperty("contentType", "text/html;charset=utf-8"));
		configParams.put("classNamePath", prop.getProperty("classNamePath", ""));
		configParams.put("defaultControllerName", prop.getProperty("defaultControllerName", "index"));
		configParams.put("defaultActionName", prop.getProperty("defaultActionName", "index"));
		configParams.put("defaultErrorController", prop.getProperty("defaultErrorController", "error"));
		configParams.put("defaultErrorAction", prop.getProperty("defaultErrorAction", "index"));
		configParams.put("defaultControllerSuffix", prop.getProperty("defaultControllerSuffix", "Controller"));
		configParams.put("defaultActionSuffix", prop.getProperty("defaultActionSuffix", "Action"));
		configParams.put("allowPathParams", prop.getProperty("allowPathParams", "0"));
	}
	
	public String getConfigParam(String key) {
		return configParams.get(key);
	}
}
