package com.iisquare.jw.demo;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Controller;

import com.iisquare.jw.core.component.CoreController;

@Controller
@Scope("prototype")
public class ErrorController extends CoreController {

	public Object indexAction(Exception e) {
		System.out.println("something is wrong!");
		return null;
	}
	
}
