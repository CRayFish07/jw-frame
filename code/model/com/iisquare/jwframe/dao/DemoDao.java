package com.iisquare.jwframe.dao;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.iisquare.jwframe.mvc.MySQLBase;

@Component
@Scope("prototype")
public class DemoDao extends MySQLBase {

	@Override
	public String tableName() {
		return tablePrefix() + "demo";
	}

}
