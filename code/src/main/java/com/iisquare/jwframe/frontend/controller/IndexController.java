package com.iisquare.jwframe.frontend.controller;

import org.apache.commons.lang.StringEscapeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Controller;

import com.iisquare.jwframe.core.component.CoreController;
import com.iisquare.jwframe.service.DemoService;
import com.iisquare.jwframe.utils.DPUtil;

@Controller
@Scope("prototype")
public class IndexController extends CoreController {

	@Autowired
	public DemoService demoService;
	
	public Object indexAction() throws Exception {
		assign("message", demoService.getMessage());
		return displayTemplate();
	}
	
	public Object newsAction() throws Exception {
		assign("date", StringEscapeUtils.escapeHtml(getParam("date")));
		assign("id", DPUtil.parseInt(getParam("id")));
		return displayMessage(0, "news working!", assign);
	} 
}
