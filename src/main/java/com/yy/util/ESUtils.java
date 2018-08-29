package com.yy.util;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.alibaba.fastjson.JSONObject;

/**
 * 
 * @author magi.qi
 *
 */
@SuppressWarnings("unchecked")
public class ESUtils {

	private static Logger log = Logger.getLogger(ESUtils.class.getSimpleName());

	/**
	 * es服务器的host
	 */
	// private static final String[] hosts = { "10.43.160.10", "10.43.160.13" };
	private static final String[] hosts = PropertiesReader.getProperty("esip")
			.split(",");
	private static final String clusterName = PropertiesReader.getProperty("clusterName");
	
	// private static final String[] hosts = { "10.3.1.103" };
	/**
	 * es服务器暴露给client的port
	 */
	private static final int port = 9300;

	private static Settings settings = Settings.settingsBuilder()
			.put("cluster.name", clusterName)
			.put("client.transport.sniff", true).put("number_of_replicas", 0)
			.put("number_of_shards", 6).build();

	/**
	 * 
	 */
	private static TransportClient client;

	static {
		try {
			client = TransportClient.builder().settings(settings).build();
			for (String string : hosts) {
				client.addTransportAddress(new InetSocketTransportAddress(
						InetAddress.getByName(string), port));
			} 
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @author Liwd
	 * @date 2016年12月7日 下午6:10:51
	 * @Description: 统计查询的总数
	 * @throws
	 */
	public static long getMultiCount(List<EsSearchBean> esSearchBeans)
			throws Exception {
		MultiSearchRequestBuilder msrb = getClient().prepareMultiSearch();
		SearchRequestBuilder srb;
		long result = 0;
		for (EsSearchBean esb : esSearchBeans) {
			srb = getClient().prepareSearch(esb.getIndexName()).setTypes(
					esb.getType());
			srb = srb.setQuery(handleQueryMap(srb, esb.getQueryMap()))
					.setSize(0).setSearchType(SearchType.QUERY_THEN_FETCH);
			msrb.add(srb);
		}
		MultiSearchResponse msp = msrb.execute().actionGet();
		for (MultiSearchResponse.Item item : msp.getResponses()) {
			SearchResponse response = item.getResponse();
			result += response.getHits().totalHits();
		}
		return result;
	}

	/**
	 * 
	 * @author Liwd
	 * @date 2016年12月12日 下午3:42:26
	 * @Description: 获取MultiSearchRequestBuilder，为后面的需要获得指定数据
	 * @throws
	 */
	public static MultiSearchResponse getMultiSearchResponse(
			List<EsSearchBean> esSearchBeans, int size) throws Exception {
		MultiSearchRequestBuilder msrb = ESUtils.getClient()
				.prepareMultiSearch();
		SearchRequestBuilder srb;
		for (EsSearchBean esb : esSearchBeans) {
			srb = ESUtils.getClient().prepareSearch(esb.getIndexName())
					.setTypes(esb.getType());
			srb = srb.setQuery(ESUtils.handleQueryMap(srb, esb.getQueryMap()))
					.setSize(size);
			msrb.add(srb);
		}

		MultiSearchResponse msp = msrb.execute().actionGet();
		return msp;
	}

	public static SearchHit[] getSearchHits(String index, String type,
			Map<String, Object> query) throws Exception {
		SearchRequestBuilder srb = ESUtils.getClient().prepareSearch(index)
				.setTypes(type);
		SearchResponse response = srb
				.setQuery(ESUtils.handleQueryMap(srb, query)).execute()
				.actionGet();
		SearchHit[] hits = response.getHits().getHits();
		return hits;
	}

	@Deprecated
	public static SearchResponse aggregation(String index, String type,
			String mode, String newName, String fieldName) throws Exception {
		SearchRequestBuilder sbuilder = getClient().prepareSearch(index)
				.setTypes(type);
		SearchResponse response = null;
		if (mode.equals(ESUtils.COUNT)) {
			TermsBuilder teamAgg = AggregationBuilders.terms(newName).field(
					fieldName);
			response = sbuilder.addAggregation(teamAgg).execute().actionGet();
		}
		return response;

	}

	/**
	 * 获得连接
	 * 
	 * @return
	 * @throws Exception
	 */
	public static Client getClient() throws Exception {
		return client;
	}

	/**
	 * 创建索引
	 * 
	 * @param index
	 * @param typeName
	 * @param mapping
	 * @throws Exception
	 */
	public static void createIndex(String index, String typeName,
			XContentBuilder mapping) throws Exception {
		Client client = getClient();
		// 如果存在就先删除索引
		if (client.admin().indices().prepareExists(index).get().isExists()) {
			throw new Exception("index::" + index + " is exists");
		}
		if (typeName == null || "".equals(typeName)) {
			// index 必须符合约定规范 xxxx_1
			typeName = index.split("_")[0];
		}

		client.admin().indices().prepareCreate(index).setSettings(settings)
				.addMapping(typeName, mapping).get();

	}

	public static void delIndex(String index) throws Exception {
		Client client = getClient();
		client.admin().indices().prepareDelete(index).execute().actionGet();

	}

	/**
	 * 查询
	 * 
	 * @param index
	 * @param type
	 * @param query
	 * @param clazz
	 * @return
	 * @throws Exception
	 */
	public static <T> List<T> query(String index, String type,
			Map<String, Object> query, Class<T> clazz) throws Exception {

		SearchRequestBuilder srb = getClient().prepareSearch(index).setTypes(
				type);
		SearchResponse response = srb.setQuery(handleQueryMap(srb, query))
				.setFrom(0).setSize(30).execute().actionGet();
		SearchHit[] hits = response.getHits().getHits();

		List<T> os = new ArrayList<T>();
		for (SearchHit hit : hits) {
			//System.out.println(hit.getSourceAsString());
			Object o = JSONObject.parseObject(hit.getSourceAsString(), clazz);
			os.add((T) o);
		}
		return os;
	}

	/**
	 * 分页,还没测试好
	 * 
	 * @param index
	 * @param type
	 * @param query
	 * @param clazz
	 * @param start
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	public static <T> List<T> queryPage(String index, String type,
			Map<String, Object> query, Class<T> clazz, int start, int limit)
			throws Exception {

		SearchRequestBuilder srb = getClient().prepareSearch(index).setTypes(
				type);
		if (limit > 100) {
			limit = 30;
		}
		SearchResponse response = srb.setQuery(handleQueryMap(srb, query))
				.setFrom(start).setSize(limit).execute().actionGet();

		SearchHit[] hits = response.getHits().getHits();
		List<T> os = new ArrayList<T>();
		for (SearchHit hit : hits) {
			Object o = JSONObject.parseObject(hit.getSourceAsString(), clazz);
			os.add((T) o);
		}
		return os;
	}

	/**
	 * 测试用
	 * 
	 * @param index
	 * @param type
	 * @param entitys
	 * @return
	 * @throws Exception
	 */
	public static boolean addBatch(String index, String type,
			List<Object> entitys) throws Exception {
		if (entitys == null || entitys.isEmpty()) {

			System.out.println("entitys is null or size is 0");
			return false;
		}
		Client client = getClient();
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		for (Object mr : entitys) {
			byte[] json = JSONObject.toJSONString(mr).getBytes();
			bulkRequest.add(client.prepareIndex(index, type,
					UUIDUtil.generateUUIDString()).setSource(json));
		}
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		boolean flag = bulkResponse.hasFailures();
		if (flag) {
			throw new Exception(bulkResponse.buildFailureMessage());
		}
		return !flag;
	}

	/**
	 * 
	 * @param index
	 * @param type
	 * @param entitys
	 * @return
	 * @throws Exception
	 */
	public static boolean addBatchUseEntityId(String index, String type,
			String pkName, List<Object> entitys) throws Exception {
		if (entitys == null || entitys.isEmpty()) {
			log.warning("entitys is null or size is 0");
			return false;
		}
		Client client = getClient();
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		for (Object mr : entitys) {
			String jsonstr = JSONObject.toJSONString(mr);
			String pkId = JSONObject.parseObject(jsonstr).getString(pkName);
			bulkRequest.add(client.prepareIndex(index, type, pkId).setSource(
					jsonstr.getBytes("UTF-8")));
		}
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		boolean flag = bulkResponse.hasFailures();
		if (flag) {
			throw new Exception(bulkResponse.buildFailureMessage());
		}
		return !flag;
	}

	/**
	 * 创建或更新document
	 * 
	 * @param index
	 *            索引
	 * @param type
	 *            类型
	 * @param id
	 *            id
	 * @param entity
	 *            保存的实体
	 * @throws Exception
	 */
	public static boolean createOrUpdateDocument(String index, String type,
			String id, Object entity) throws Exception {
		try {
			byte[] json = JSONObject.toJSONString(entity).getBytes("UTF-8");
			IndexRequestBuilder irb = getClient().prepareIndex(index, type, id)
					.setSource(json);
			irb.get();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * 删除document
	 * 
	 * @param index
	 * @param type
	 * @param id
	 * @throws Exception
	 */
	public static boolean deleteDocument(String index, String type, String id)
			throws Exception {
		try {
			getClient().prepareDelete(index, type, id).get();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * count方法
	 * 
	 * @param index
	 * @param type
	 * @param query
	 * @return
	 * @throws Exception
	 */
	public static long count(String index, String type,
			Map<String, Object> query) throws Exception {
		SearchRequestBuilder srb = getClient().prepareSearch(index);
		if (type != null && !"".equals(type)) {
			srb.setTypes(type);
		}

		SearchResponse response = srb.setQuery(handleQueryMap(srb, query))
				.setSize(0).setSearchType(SearchType.QUERY_THEN_FETCH)
				.execute().actionGet();

		return response.getHits().getTotalHits();
	}

	public static void updateNumberOfReplicasSetting(String index)
			throws Exception {
		getClient()
				.admin()
				.indices()
				.prepareUpdateSettings(index)
				.setSettings(
						Settings.builder().put("index.number_of_replicas", 1))
				.get();
	}

	/**
	 * 处理查询条件 isNull 暂还没找到替代方法
	 * 
	 * @param srb
	 * @param query
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static BoolQueryBuilder handleQueryMap(SearchRequestBuilder srb,
			Map<String, Object> query) {
		BoolQueryBuilder main = new BoolQueryBuilder();

		BoolQueryBuilder bqb = new BoolQueryBuilder();
		for (Entry<String, Object> obj : query.entrySet()) {
			if (obj.getValue() == null || "".equals(obj.getValue())) {// 为空.就不操作
				continue;
			}
			if (obj.getKey().endsWith(EQUAL)) {
				String key = obj.getKey().replace(EQUAL, EMPTY);

				if (key.contains(".")) {
					BoolQueryBuilder sbqb = new BoolQueryBuilder();
					sbqb.filter(QueryBuilders.termQuery(key, obj.getValue()));
					main.filter(new NestedQueryBuilder(key.split("[.]")[0],
							sbqb));
					continue;
				}

				bqb.filter(QueryBuilders.termQuery(key, obj.getValue()));
				continue;
			}
			if (obj.getKey().endsWith(LIKE)) {
				String key = obj.getKey().replace(LIKE, EMPTY);
				if (key.contains(".")) {
					BoolQueryBuilder sbqb = new BoolQueryBuilder();
					sbqb.filter(QueryBuilders.prefixQuery(key,
							(String) obj.getValue()));
					main.filter(new NestedQueryBuilder(key.split("[.]")[0],
							sbqb));
					continue;
				}
				// 该模糊查询只提供前缀匹配
				bqb.filter(QueryBuilders.prefixQuery(key,
						(String) obj.getValue()));
				continue;
			}
			if (obj.getKey().endsWith(FUZZY)) {
				String key = obj.getKey().replace(FUZZY, EMPTY);
				if (key.contains(".")) {
					BoolQueryBuilder sbqb = new BoolQueryBuilder();
					sbqb.filter(QueryBuilders.termsQuery(key,
							(List<?>) obj.getValue()));
					main.filter(new NestedQueryBuilder(key.split("[.]")[0],
							sbqb));
					continue;
				}
				bqb = bqb.filter(QueryBuilders.fuzzyQuery(key, obj.getValue()));
				continue;
			}
			if (obj.getKey().endsWith(IN)) {// test
				String key = obj.getKey().replace(IN, EMPTY);
				if (key.contains(".")) {
					BoolQueryBuilder sbqb = new BoolQueryBuilder();
					sbqb.filter(QueryBuilders.termsQuery(key, obj.getValue()));
					main.filter(new NestedQueryBuilder(key.split("[.]")[0],
							sbqb));
					continue;
				}
				bqb = bqb.filter(QueryBuilders.termsQuery(key,
						(List<?>) obj.getValue()));
				continue;
			}
			if (obj.getKey().endsWith(MORE)) {// test
				String key = obj.getKey().replace(MORE, EMPTY);
				if (key.contains(".")) {
					BoolQueryBuilder sbqb = new BoolQueryBuilder();
					sbqb.filter(QueryBuilders.rangeQuery(key)
							.gt(obj.getValue()));
					main.filter(new NestedQueryBuilder(key.split("[.]")[0],
							sbqb));
					continue;
				}
				bqb = bqb.filter(QueryBuilders.rangeQuery(key).gt(
						obj.getValue()));
				continue;
			}

			if (obj.getKey().endsWith(MOREEQUAL)) {// test
				String key = obj.getKey().replace(MOREEQUAL, EMPTY);
				if (key.contains(".")) {
					BoolQueryBuilder sbqb = new BoolQueryBuilder();
					sbqb.filter(QueryBuilders.rangeQuery(key).gte(
							obj.getValue()));
					main.filter(new NestedQueryBuilder(key.split("[.]")[0],
							sbqb));
					continue;
				}
				bqb.filter(QueryBuilders.rangeQuery(key).gte(obj.getValue()));
				continue;
			}
			if (obj.getKey().endsWith(LESS)) {// test
				String key = obj.getKey().replace(LESS, EMPTY);
				if (key.contains(".")) {
					BoolQueryBuilder sbqb = new BoolQueryBuilder();
					sbqb.filter(QueryBuilders.rangeQuery(key)
							.lt(obj.getValue()));
					main.filter(new NestedQueryBuilder(key.split("[.]")[0],
							sbqb));
					continue;
				}
				bqb = bqb.filter(QueryBuilders.rangeQuery(key).lt(
						obj.getValue()));
				continue;
			}
			if (obj.getKey().endsWith(LESSEQUAL)) {// test
				String key = obj.getKey().replace(LESSEQUAL, EMPTY);
				if (key.contains(".")) {
					BoolQueryBuilder sbqb = new BoolQueryBuilder();
					sbqb.filter(QueryBuilders.rangeQuery(key).lte(
							obj.getValue()));
					main.filter(new NestedQueryBuilder(key.split("[.]")[0],
							sbqb));
					continue;
				}
				bqb.filter(QueryBuilders.rangeQuery(key).lte(obj.getValue()));
				continue;
			}

			if (obj.getKey().endsWith(BETWEEN)) {
				String key = obj.getKey().replace(BETWEEN, EMPTY);
				Object[] dates = (Object[]) obj.getValue();
				if (key.contains(".")) {
					BoolQueryBuilder sbqb = new BoolQueryBuilder();
					sbqb.filter(QueryBuilders.rangeQuery(key).gt(dates[0])
							.lt(dates[1]));
					main.filter(new NestedQueryBuilder(key.split("[.]")[0],
							sbqb));
					continue;
				}

				bqb.filter(QueryBuilders.rangeQuery(key).gt(dates[0])
						.lt(dates[1]));
				continue;
			}
			if (obj.getKey().endsWith(ISNOTNULL)) {
				String key = obj.getKey().replace(ISNOTNULL, EMPTY);
				if (key.contains(".")) {
					BoolQueryBuilder sbqb = new BoolQueryBuilder();
					sbqb.filter(QueryBuilders.existsQuery(key));
					main.filter(new NestedQueryBuilder(key.split("[.]")[0],
							sbqb));
					continue;
				}
				bqb.filter(QueryBuilders.existsQuery(key));
				continue;
			}
			if (obj.getKey().endsWith(ISNULL)) {
				String key = obj.getKey().replace(ISNULL, EMPTY);
				if (key.contains(".")) {
					BoolQueryBuilder sbqb = new BoolQueryBuilder();
					sbqb.filter(QueryBuilders.missingQuery(key));
					main.filter(new NestedQueryBuilder(key.split("[.]")[0],
							sbqb));
					continue;
				}
				bqb.filter(QueryBuilders.missingQuery(key));
				continue;
			}

			if (obj.getKey().endsWith(ORDER_DESC)) {
				String key = obj.getKey().replace(ORDER_DESC, EMPTY);
				srb.addSort(key, SortOrder.DESC);
				continue;
			}

			if (obj.getKey().endsWith(ORDER_ASC)) {
				String key = obj.getKey().replace(ORDER_ASC, EMPTY);
				srb.addSort(key, SortOrder.ASC);
				continue;
			}

			if (obj.getKey().endsWith(OR)) {
				String key = obj.getKey().replace(OR, EMPTY);
				String[] ks = key.split("[|]");
				for (String a : ks) {
					bqb = bqb
							.should(QueryBuilders.termQuery(a, obj.getValue()));
				}
			}
		}
		main.filter(bqb);
		return main;
	}

	public static void main2(String[] args) throws Exception {

		System.out.println(getESIndexOrTypeName("T_SH_KHJSYCYZGZXX"));

		System.out.println(getEsColumnName("F_Real_Relevancy"));

	}

	/**
	 * ORACLE 和ES对应关系
	 * 
	 * @param columName
	 * @return
	 */
	public static String getEsColumnName(String columName) {
		String paramName = "";
		String[] names = columName.substring(2, columName.length()).split("_");
		if (names.length >= 2) {
			for (String string : names) {
				String s = string.toLowerCase();
				paramName += toUpperCaseFirstOne(s);
			}
		} else {
			paramName = columName.substring(2, columName.length())
					.toLowerCase();
		}
		return toLowerCaseFirstOne(paramName);
	}

	/**
	 * index和type名字一致,index最终需要加后缀_1,2,3,4...
	 * 
	 * @param tblName
	 * @return
	 */
	public static String getESIndexOrTypeName(String tblName) {
		String paramName = "";
		String[] names = tblName.substring(2, tblName.length()).split("_");
		if (names.length >= 2) {
			for (String string : names) {
				String s = string.toLowerCase();
				paramName += s;
			}
		} else {
			paramName = tblName.substring(2, tblName.length()).toLowerCase();
		}
		return toLowerCaseFirstOne(paramName);
	}

	/**
	 * 首写字母大写
	 * 
	 * @param s
	 * @return
	 */
	private static String toUpperCaseFirstOne(String s) {
		if (Character.isUpperCase(s.charAt(0)))
			return s;
		else
			return (new StringBuilder())
					.append(Character.toUpperCase(s.charAt(0)))
					.append(s.substring(1)).toString();
	}

	/**
	 * 首写字母小写
	 * 
	 * @param s
	 * @return
	 */
	private static String toLowerCaseFirstOne(String s) {
		if (Character.isLowerCase(s.charAt(0)))
			return s;
		else
			return (new StringBuilder())
					.append(Character.toLowerCase(s.charAt(0)))
					.append(s.substring(1)).toString();
	}

	private static final String EMPTY = "";

	public static String EQUAL = "$Equal$";
	public static String NOTEQUAL = "$NotEqual$";
	public static String ISNULL = "$isNull$";
	public static String ISNOTNULL = "$isNotNull$";
	public static String LIKE = "$Like$";
	public static String MOREEQUAL = "$MoreEqual$";
	public static String LESSEQUAL = "$LessEqual$";
	public static String MORE = "$More$";
	public static String LESS = "$Less$";
	public static String IN = "$In$";
	public static String BETWEEN = "$bewteen$";
	public static String ORDER_DESC = "$OrderDesc$";
	public static String ORDER_ASC = "$OrderAsc$";
	public static String COUNT = "$Count$";
	public static String FUZZY = "$Fuzzy$";
	public static String OR = "$Or$";
	
	
	public static void main(String[] args) {
//		String index="qwjs_1";
//		String type="qwjs";
//		List<Object> eqList=new ArrayList<Object>();
//		EsQwjs eq=new EsQwjs();
//		eq.setId(UUIDUtil.generateUUIDString());
//		
//		SimpleDateFormat sdf = new SimpleDateFormat(
//				"yyyy-MM-dd HH:mm:ss");
//		
//		eq.setCreateTime(sdf.format(new Date()));
//		eq.setMappingCode("11");
//		eq.setMappingFileId("aa");
//		eq.setAll("姓名:22 性别:男 地址:广州");
//		eqList.add(eq);
//		try {
//			boolean flag = ESUtils.addBatchUseEntityId(index,
//					type.toLowerCase(), "id", eqList);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		
		 File file = new File("C:\\Users\\suguowei\\Desktop\\test\\uploadToEs\\11\\11.ready");
	        String strParentDirectory = file.getParent();
	        System.out.println("文件的上级目录为 : " + strParentDirectory.replace("C:\\Users\\suguowei\\Desktop\\test\\uploadToEs\\", ""));
	}

}
