package com.iisquare.jwframe.dao;

import java.util.LinkedHashMap;
import java.util.Map;

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

	@Override
	public LinkedHashMap<String, Map<String, Object>> columns() {
		LinkedHashMap<String, Map<String, Object>> columns = new LinkedHashMap<>();
		columns.put("id", null); // 主键
		columns.put("parent_id", null); // 父级
		columns.put("name", null); // 名称
		columns.put("status", null); // 状态
		return columns;
	}

}
