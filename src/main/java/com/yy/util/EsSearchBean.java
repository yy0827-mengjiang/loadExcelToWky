package com.yy.util;

import java.util.Map;

/**
 * 用于es中批量查询中的实体
 * @author lenovo
 *
 */
public class EsSearchBean {
	private String indexName;
	private String type;
	private Map<String, Object> queryMap;
	private Class<?> clazz;
	
	public EsSearchBean() {
		super();
	}
	public EsSearchBean(String indexName, String type,
			Map<String, Object> queryMap) {
		super();
		this.indexName = indexName;
		this.type = type;
		this.queryMap =  queryMap;
	}
	public EsSearchBean(String indexName, String type,
			Map<String, Object> queryMap, Class<?> clazz) {
		super();
		this.indexName = indexName;
		this.type = type;
		this.queryMap = queryMap;
		this.clazz = clazz;
	}
	public Class<?> getClazz() {
		return clazz;
	}
	public void setClazz(Class<?> clazz) {
		this.clazz = clazz;
	}
	public String getIndexName() {
		return indexName;
	}
	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public Map<String, Object> getQueryMap() {
		return queryMap;
	}
	public void setQueryMap(Map<String, Object> queryMap2) {
		this.queryMap = queryMap2;
	}
	
}
