package com.yy.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;



public class SolrUtil {
	private static String zkHost = PropertiesReader.getProperty("solrZkHost");
	private static String defaultCollection = PropertiesReader.getProperty("nasQwjsCollection");
	private static CloudSolrClient server = null;
	private static Logger logger = Logger
			.getLogger(SolrUtil.class);

	public static void init() {
		if(server == null){
			server = new CloudSolrClient(zkHost);
			server.setDefaultCollection(defaultCollection);
		}
	}
	
	public static void OnceAddDocList(List<JSONObject> solrJsonList) throws SolrServerException,
	IOException, JSONException{
		List<SolrInputDocument> addMapList = new ArrayList<SolrInputDocument>();

		for (int i = 0; i < solrJsonList.size(); i++) {
			
			JSONObject solrDocJson = solrJsonList.get(i);
			SolrInputDocument doc = new SolrInputDocument();
			if(solrDocJson.containsKey("mappingCode")){
				doc.addField("mappingCode", solrDocJson.getString("mappingCode"));
			}
			
			if(solrDocJson.containsKey("mappingFileId")){
				doc.addField("mappingFileId", solrDocJson.getString("mappingFileId"));
			}
			
			if(solrDocJson.containsKey("all")){
				doc.addField("all", solrDocJson.getString("all"));
			}
			
			if(solrDocJson.containsKey("createTime")){
				doc.addField("createTime", solrDocJson.getString("createTime"));
			}
			doc.addField("id", UUIDUtil.generateUUIDString());

			addMapList.add(doc);

		}
		logger.info("插入多少条:" + addMapList.size());
		if (addMapList != null && addMapList.size() > 0) {
				addByList(addMapList);
		}
	}
	
	public static void addByList(List<SolrInputDocument> docList)
			throws SolrServerException, IOException {
		try {
			UpdateResponse response = server.add(docList, 50);
			if (response.getStatus() == 0) {
				logger.info("insert successfully!");
			} else {
				logger.error("insert failed!");
			}
			server.commit();
		} catch (Exception e) {
			logger.error("addByList", e);;
			e.printStackTrace();
		}
	}

	public String getZkHost() {
		return zkHost;
	}

	public void setZkHost(String zkHost) {
		this.zkHost = zkHost;
	}

	public String getDefaultCollection() {
		return defaultCollection;
	}

	public void setDefaultCollection(String defaultCollection) {
		this.defaultCollection = defaultCollection;
	}

	public CloudSolrClient getServer() {
		return server;
	}

	public void setServer(CloudSolrClient server) {
		this.server = server;
	}
	
	
	

}
