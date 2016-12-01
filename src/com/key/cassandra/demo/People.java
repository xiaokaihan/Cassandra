package com.key.cassandra.demo;

/**
 * 
 * people entity class for CassandraDemo.
 * @author Key.Xiao
 *
 */
public class People {

	private int id;
	
	private String fname;
	
	private String lname;
	
	private String gender;
	
	private int age;
	
	private String email;
	
	public People() {
		// TODO Auto-generated constructor stub
	}
	
	public People (int id, String fname, String lname, String gender, int age, String email) {
		this.id = id;
		this.fname = fname;
		this.lname = lname;
		this.gender = gender;
		this.age = age;
		this.email = email;
	}

	public int getId() {
		return id;
	}

	public String getFname() {
		return fname;
	}

	public String getLname() {
		return lname;
	}

	public String getGender() {
		return gender;
	}

	public int getAge() {
		return age;
	}

	public String getEmail() {
		return email;
	}
	
}
