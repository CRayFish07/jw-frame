package com.iisquare.jwframe.service;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.iisquare.jwframe.mvc.ServiceBase;

@Service
@Scope("singleton")
public class DemoService extends ServiceBase {

	public String getMessage() {
        return "it works!";
    }
	
}
