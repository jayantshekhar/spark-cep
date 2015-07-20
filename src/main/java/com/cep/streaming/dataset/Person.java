package com.cep.streaming.dataset;

import java.io.Serializable;

/**
 * Created by jayant on 6/11/15.
 */
public class Person implements Serializable {
    private String name;
    private int age;

    public Person() {

    }
    public Person(String str) {
        String[] parts = str.split(",");
        setName(parts[0]);
        setAge(Integer.parseInt(parts[1].trim()));

    }
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
