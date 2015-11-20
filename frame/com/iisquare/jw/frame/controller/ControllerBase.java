package com.iisquare.jw.frame.controller;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.context.WebApplicationContext;

import com.iisquare.jw.frame.FrameConfiguration;
import com.iisquare.jw.frame.view.FreeMarkerConfigurer;

import freemarker.template.Configuration;
import freemarker.template.Template;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public abstract class ControllerBase {
	protected WebApplicationContext wac;
	protected String appUri, appUrl, appPath, rootPath;
	protected String controllerName, actionName;

	protected HttpServletRequest request;
	protected HttpServletResponse response;

	protected Map<String, String[]> params; // 请求参数
	protected Map<String, Object> assign; // 视图数据Map对象

	public Object init() {
		return null;
	}

	public Object destroy(Object actionVal) {
		return actionVal;
	}

	/**
	 * 设置视图中需要的参数
	 */
	protected void assign(String key, Object value) {
		assign.put(key, value);
	}

	public String url() {
		return url(controllerName, actionName);
	}

	public String url(String action) {
		return url(controllerName, action);
	}

	/**
	 * 获取URL地址
	 */
	public String url(String controller, String action) {
		return appPath + controller + "/" + action;
	}

	protected Object displayTemplate() throws Exception {
		return displayTemplate(controllerName, actionName);
	}
	
	protected Object displayTemplate(String action) throws Exception {
		return displayTemplate(controllerName, action);
	}
	
	protected Object displayTemplate(String controller, String action) throws Exception {
		return displayTemplate(controller + "/" + action, assign);
	}
	
	protected Object displayTemplate(String fileUri, Object dataModel) throws Exception {
		Configuration cfg = wac.getBean(FreeMarkerConfigurer.class).getConfiguration();
		FrameConfiguration frameConfiguration = wac.getBean(FrameConfiguration.class);
        Template template = cfg.getTemplate(fileUri + frameConfiguration.getTemplateSuffix());
        Writer out = new OutputStreamWriter(response.getOutputStream());
        template.process(assign, out);
		return null;
	}
	
	/**
	 * 输出文本信息
	 */
	protected Object displayText(String text) throws Exception {
		PrintWriter out = response.getWriter();
		out.print(text);
		out.flush();
		return null;
	}

	/**
	 * 将assign中的数据输出为JSON格式
	 */
	protected Object displayJSON() throws Exception {
		return displayJSON(assign);
	}

	/**
	 * 输出JSON信息
	 */
	protected Object displayJSON(Object object) throws Exception {
		String result;
		if (object instanceof Map) {
			result = JSONObject.fromObject(object).toString();
		} else {
			result = JSONArray.fromObject(object).toString();
		}
		return displayText(result);
	}

	/**
	 * 重定向自定义URL地址
	 */
	protected Object redirect(String url) throws Exception {
		response.sendRedirect(url);
		return null;
	}
}
