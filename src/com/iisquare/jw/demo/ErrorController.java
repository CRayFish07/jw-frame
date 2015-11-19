package com.iisquare.jw.demo;

import com.iisquare.jw.core.component.CoreController;

public class ErrorController extends CoreController {

	public Object indexAction(Exception e) {
		System.out.println("something is wrong!");
		return null;
	}
	
}
