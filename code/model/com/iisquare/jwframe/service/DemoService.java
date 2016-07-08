package com.iisquare.jwframe.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.iisquare.jwframe.dao.DemoDao;
import com.iisquare.jwframe.mvc.ServiceBase;

@Service
@Scope("singleton")
public class DemoService extends ServiceBase {

	@Autowired
	public DemoDao demoDao;
	
	/*public function insert($data) {
        return $this->demoDao->insert($data);
    }
    
    public function getList() {
        return $this->demoDao->where(['status' => 1])->orderBy('id desc')->limit(30)->all();
    }*/
	
	public void getList() {
		System.out.println(demoDao.tableName());
	}
	
	public String getMessage() {
        return "it works!";
    }
	
}
