package com.iisquare.jwframe.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.iisquare.jwframe.dao.DemoDao;
import com.iisquare.jwframe.mvc.ServiceBase;

@Service
@Scope("prototype")
public class DemoService extends ServiceBase {

	@Autowired
	public DemoDao demoDao;
	
	public Integer insert(Map<String, Object> data) {
		Number id = demoDao.insert(data);
		return null == id ? null : id.intValue();
    }
    
	public List<Map<String, Object>> getList() {
		return demoDao.where("status=1").orderBy("id desc").limit(30).all();
	}
	
	public String getMessage() {
        return "it works!";
    }
	
}
