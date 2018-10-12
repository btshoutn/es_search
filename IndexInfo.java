package com.elastic.service.impl;

import java.util.Date;
import java.util.Map;

/**
 * Created by xiaotian on 2017/12/15.
 */
public class IndexInfo {
    private String name;
    private Integer age;
    private Date create_date;
    private String message;
    private String tel;
    private String[] attr_name;
    private Map<String,Object> attrMap;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Date getCreate_date() {
        return create_date;
    }

    public void setCreate_date(Date create_date) {
        this.create_date = create_date;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String[] getAttr_name() {
        return attr_name;
    }

    public void setAttr_name(String[] attr_name) {
        this.attr_name = attr_name;
    }

    public Map<String, Object> getAttrMap() {
        return attrMap;
    }

    public void setAttrMap(Map<String, Object> attrMap) {
        this.attrMap = attrMap;
    }

}
