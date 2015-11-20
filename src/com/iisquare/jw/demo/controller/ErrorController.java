package com.iisquare.jw.demo.controller;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Controller;

import com.iisquare.jw.core.component.CoreController;

@Controller
@Scope("prototype")
public class ErrorController extends CoreController {

	public Object indexAction(Exception e) throws Exception {
		return displayTemplate();
	}
	
}
