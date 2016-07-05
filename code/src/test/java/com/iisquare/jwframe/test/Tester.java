package com.iisquare.jwframe.test;

import java.util.List;

import com.iisquare.jwframe.utils.DPUtil;

public class Tester {

	public static void main(String[] args) {
		String route = "/news/2015-06-07/124.shtml";
		String uri = "/news/{date}/{id}.shtml";
		uri = "^" + uri.replaceAll("\\.", "\\\\.").replaceAll("\\{\\w+\\}", "(.*?)") + "$";
		List<String> matches = DPUtil.getMatcher(uri, route, true);
		System.out.println(uri);
        System.out.println(matches.subList(1, matches.size()));
	}

}
