package com.kafka.demo;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.kafka.utils.PropertiesLoader;

public class Test {
	public final static String PROJ_HOME = Producer.class.getClassLoader()
			.getResource("").getPath().replace("classes/", "")
			.replace("target/", "").replace("test-", "");

	public static void main(String[] args) {
		System.out.println(PROJ_HOME);
		try {
			List<String> json = FileUtils.readLines(new File(PROJ_HOME
					+ "data/group_json"), "UTF-8");
			List<String> json1 = FileUtils.readLines(new File(PROJ_HOME
					+ "data/group_json1"), "UTF-8");
			run(json, json1);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void run(List<String> json, List<String> json1) {
		PropertiesLoader loader = new PropertiesLoader(
				new String[] { "kafka.properties" });
		Producer producerThread = new Producer(loader.getProperty(
				"kafka.topic").split(",")[0], json);
		producerThread.start();

		Producer producerThread1 = new Producer(loader.getProperty(
				"kafka.topic").split(",")[1], json1);
		producerThread1.start();
	}
}
