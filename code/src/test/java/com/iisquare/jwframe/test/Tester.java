package com.iisquare.jwframe.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Tester {

	public static void main(String[] args) throws Exception {
	    List<Integer> list = new ArrayList<>();
	    for (int i = 0; i <= 1; i++) {
	    	list.add(i);
	    }
	    System.out.println(list.get(new Random().nextInt(list.size())));
	}

}
