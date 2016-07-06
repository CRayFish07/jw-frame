package com.iisquare.jwframe.frontend.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Controller;

import com.iisquare.jwframe.core.component.CoreController;
import com.iisquare.jwframe.service.DemoService;

@Controller
@Scope("prototype")
public class IndexController extends CoreController {

	@Autowired
	public DemoService demoService;
	
	public Object indexAction () throws Exception {
		assign("message", demoService.getMessage());
		return displayTemplate();
	}
	
}
