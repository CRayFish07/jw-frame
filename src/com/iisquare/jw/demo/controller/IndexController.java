package com.iisquare.jw.demo.controller;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Controller;

import com.iisquare.jw.core.component.CoreController;

@Controller
@Scope("prototype")
public class IndexController extends CoreController {

	public Object indexAction() throws Exception {
		assign("message", "it works!");
		return displayTemplate();
	}
	
}
