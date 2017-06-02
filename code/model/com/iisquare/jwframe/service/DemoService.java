package com.iisquare.jwframe.service;

import java.util.List;
import java.util.Map;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.iisquare.jwframe.dao.DemoDao;
import com.iisquare.jwframe.mvc.ServiceBase;

@Service
@Scope("singleton")
public class DemoService extends ServiceBase {

	public Integer insert(Map<String, Object> data) {
		DemoDao demoDao = new DemoDao();
		Number id = demoDao.insert(data);
		return null == id ? null : id.intValue();
    }
    
	public List<Map<String, Object>> getList() {
		DemoDao demoDao = new DemoDao();
		return demoDao.where("status=1").orderBy("id desc").limit(30).all();
	}
	
	public String getMessage() {
        return "it works!";
    }
	
}
