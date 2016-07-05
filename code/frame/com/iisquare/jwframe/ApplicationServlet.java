package com.iisquare.jwframe;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

public class ApplicationServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private WebApplicationContext wac;
	private String appUri, rootPath;

	@Override
	public void init() throws ServletException {
		wac = WebApplicationContextUtils.getRequiredWebApplicationContext(getServletContext());
		appUri = getInitParameter("appUri");
		if (!appUri.startsWith("/")) appUri = "/" + appUri;
		if (!appUri.endsWith("/")) appUri += "/";
		rootPath = getServletContext().getRealPath("/");
		System.out.println(appUri + "\r\n" + rootPath);
		super.init();
	}

	@Override
	public void destroy() {
		super.destroy();
	}

	@Override
	protected void service(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
	}

}
