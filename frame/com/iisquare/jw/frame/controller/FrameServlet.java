package com.iisquare.jw.frame.controller;

import java.io.IOException;
import java.util.LinkedHashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.iisquare.jw.frame.FrameConfiguration;

public class FrameServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private WebApplicationContext wac;
	private FrameConfiguration frameConfiguration;
	private String appUri, rootPath;

	@Override
	public void init() throws ServletException {
		wac = WebApplicationContextUtils.getRequiredWebApplicationContext(getServletContext());
		frameConfiguration = wac.getBean(FrameConfiguration.class);
		appUri = getInitParameter("appUri");
		if (!appUri.startsWith("/")) appUri = "/" + appUri;
		if (!appUri.endsWith("/")) appUri += "/";
		rootPath = getServletContext().getRealPath("/");
		super.init();
	}

	@Override
	public void destroy() {
		super.destroy();
	}

	@Override
	protected void service(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		String characterEncoding = frameConfiguration.getCharacterEncoding();
		request.setCharacterEncoding(characterEncoding);
		response.setCharacterEncoding(characterEncoding);
		response.setContentType(frameConfiguration.getContentType());
		Object[] route = parse(request);
		try {
			if (null == route) throw new Exception("uriError");
			processInvoke(request, response, route, null, 0);
		} catch (Exception e) {
			route[0] = frameConfiguration.getDefaultErrorController();
			route[1] = frameConfiguration.getDefaultErrorAction();
			try {
				processInvoke(request, response, route, e, 0);
			} catch (Exception e1) {
				proessError(request, response, route, e1, 1);
			}
		}
	}

	private void processInvoke(HttpServletRequest request, HttpServletResponse response,
			Object[] route, Exception e, int count) throws Exception {
		String controllerName = route[0].toString();
		String actionName = route[1].toString();
		Class<?> controller = Class.forName(frameConfiguration.getControllerNamePath()
				+ "." + controllerName.substring(0, 1).toUpperCase()
				+ controllerName.substring(1)
				+ frameConfiguration.getDefaultControllerSuffix());
		ControllerBase instance = (ControllerBase) wac.getBean(controller);
		instance.wac = wac;
		int port = request.getServerPort();
		String appUrl = request.getScheme() + "://" + request.getServerName();
		if (80 != port) appUrl += ":" + port;
		String appPath = appUrl + appUri;
		instance.appUrl = appUrl;
		instance.appUri = appUri;
		instance.appPath = appPath;
		instance.rootPath = rootPath;
		instance.controllerName = controllerName;
		instance.actionName = actionName;
		instance.request = request;
		instance.response = response;
		instance.params = request.getParameterMap();
		instance.assign = new LinkedHashMap<>();
		Object initVal = instance.init();
		if (null != initVal) {
			proessError(request, response, route, new Exception("initError"), count);
			return;
		}
		Object actionVal;
		if (null == e) {
			actionVal = controller.getMethod(actionName
					+ frameConfiguration.getDefaultActionSuffix()).invoke(instance);
		} else {
			actionVal = controller.getMethod(
					actionName + frameConfiguration.getDefaultActionSuffix(),
					Exception.class).invoke(instance, e);
		}
		Object destroyVal = instance.destroy(actionVal);
		if (null != destroyVal) {
			proessError(request, response, route, new Exception("destroyError"), count);
			return;
		}
	}

	private void proessError(HttpServletRequest request,
			HttpServletResponse response, Object[] route, Exception e, int count) {
		if (count > 0) {
			Log logger = LogFactory.getLog(FrameConfiguration.class);
			if(logger.isErrorEnabled()) {
				logger.error(route.toString(), e);
			}
			return;
		}
		route[0] = frameConfiguration.getDefaultErrorController();
		route[1] = frameConfiguration.getDefaultErrorAction();
		try {
			processInvoke(request, response, route, e, count++);
		} catch (Exception e1) {
			proessError(request, response, route, new Exception("proessError"), count++);
		}
	}

	private Object[] parse(HttpServletRequest request) {
		Object[] route = new Object[3];
		route[0] = frameConfiguration.getDefaultControllerName(); // controllerName
		route[1] = frameConfiguration.getDefaultActionName(); // actionName
		route[2] = ""; // paramString
		String uri = request.getRequestURI().trim();
		if (!uri.matches("^[\\/_a-zA-Z\\d\\-]*$")) return null;
		if (!uri.endsWith("/")) uri += "/";
		if (null != appUri && uri.startsWith(appUri)) uri = uri.replaceFirst(appUri, "");
		if (uri.startsWith("/")) uri = uri.substring(1);
		if (uri.endsWith("/")) uri = uri.substring(0, uri.length() - 1);
		if ("".equals(uri)) return route;
		String[] uris = uri.split("/");
		route[0] = uris[0];
		if (1 == uris.length) return route;
		route[1] = uris[1];
		if (2 == uris.length) return route;
		if (0 == frameConfiguration.getAllowPathParams()) return null;
		StringBuilder stringBuilder = new StringBuilder();
		int i, length = uris.length - 1;
		for (i = 2; i < length; i++) {
			stringBuilder.append(uris[i]).append("/");
		}
		stringBuilder.append(uris[i]);
		route[2] = stringBuilder.toString();
		return route;
	}
}