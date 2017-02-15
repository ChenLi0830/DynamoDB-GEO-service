package com.serverless.geo.function;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.serverless.demo.model.ServerlessInput;
import com.serverless.demo.model.ServerlessOutput;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.geo.GeoDataManager;
import com.amazonaws.geo.GeoDataManagerConfiguration;
import com.amazonaws.geo.model.DeletePointRequest;
import com.amazonaws.geo.model.DeletePointResult;
import com.amazonaws.geo.model.GeoPoint;
import com.amazonaws.geo.model.GeoQueryResult;
import com.amazonaws.geo.model.GetPointRequest;
import com.amazonaws.geo.model.GetPointResult;
import com.amazonaws.geo.model.PutPointRequest;
import com.amazonaws.geo.model.PutPointResult;
import com.amazonaws.geo.model.QueryRadiusRequest;
import com.amazonaws.geo.model.QueryRadiusResult;
import com.amazonaws.geo.model.QueryRectangleRequest;
import com.amazonaws.geo.model.QueryRectangleResult;
import com.amazonaws.geo.model.UpdatePointRequest;
import com.amazonaws.geo.model.UpdatePointResult;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.serverless.json.JSONException;
import com.serverless.json.JSONObject;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

public class GeoDynamoDB implements RequestHandler<ServerlessInput, ServerlessOutput> {
	private static final long serialVersionUID = 1L;

	private GeoDataManagerConfiguration config;
	private GeoDataManager geoDataManager;

	private ObjectMapper mapper;
	private JsonFactory factory;
	
	public void init() {
		setupGeoDataManager();

		mapper = new ObjectMapper();
		factory = mapper.getJsonFactory();
	}
	
	private void setupGeoDataManager() {
		String accessKey = "AKIAJPPTBPMK4MBYM2TA";
		String secretKey = "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";
		String tableName = "geo-test";
//		String regionName = "us-west-2";
		
		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		AmazonDynamoDBClient ddb = new AmazonDynamoDBClient(credentials);
		ddb.setRegion(Region.getRegion(Regions.US_WEST_2));  
		
//		Region region = Region.getRegion(Regions.fromName(regionName));
//		ddb.setRegion(region);

		config = new GeoDataManagerConfiguration(ddb, tableName);
		geoDataManager = new GeoDataManager(config);
	}
	
	@Override
    public ServerlessOutput handleRequest(ServerlessInput serverlessInput, Context context) {
        // Using builder to create the clients could allow us to dynamically load the region from the AWS_REGION environment
        // variable. Therefore we can deploy the Lambda functions to different regions without code change.
		
		init();
		
		ServerlessOutput output = new ServerlessOutput();
        
		
		try {
//			System.out.println("serverlessInput.getPath() " + serverlessInput.getPath());
			
			String path = serverlessInput.getPath().split("/")[2];
			System.out.println("path "+path);
			String action = path;
			
//			JSONObject requestObject = serverlessInput.getBody();
			
			JSONObject requestObject = new JSONObject(serverlessInput.getBody());

			if (action.equalsIgnoreCase("put-point")) {
				putPoint(requestObject);
				output.setStatusCode(200);
		        output.setBody("put-point success");
			}
//			else if (action.equalsIgnoreCase("get-point")) {
//				getPoint(requestObject, out);
//			} else if (action.equalsIgnoreCase("update-point")) {
//				updatePoint(requestObject, out);
//			} else if (action.equalsIgnoreCase("query-rectangle")) {
//				queryRectangle(requestObject, out);
//			} else if (action.equalsIgnoreCase("query-radius")) {
//				queryRadius(requestObject, out);
//			} else if (action.equalsIgnoreCase("delete-point")) {
//				deletePoint(requestObject, out);
//			}
		} catch (Exception e) {
//			StringWriter sw = new StringWriter();
//			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace();
		}

        return output;        
    }
	
	private void putPoint(JSONObject requestObject) throws IOException, JSONException {
		System.out.println("putPoint start");
		System.out.println("requestObject" + requestObject.toString());
		GeoPoint geoPoint = new GeoPoint(requestObject.getDouble("lat"), requestObject.getDouble("lng"));
//		System.out.println("lat"+requestObject.getDouble("lat"));
//		System.out.println("geoPoint" + geoPoint.toString());
		AttributeValue rangeKeyAttributeValue = new AttributeValue().withS(UUID.randomUUID().toString());
		
		AttributeValue schoolNameKeyAttributeValue = new AttributeValue().withS(requestObject.getString("schoolName"));

		System.out.println("schoolNameKeyAttributeValue " + schoolNameKeyAttributeValue.toString());
		System.out.println("rangeKeyAttributeValue " + rangeKeyAttributeValue.toString());
		
		PutPointRequest putPointRequest = new PutPointRequest(geoPoint, rangeKeyAttributeValue);
		putPointRequest.getPutItemRequest().addItemEntry("schoolName", schoolNameKeyAttributeValue);

		System.out.println(geoDataManager.toString());
		System.out.println(putPointRequest.toString());
		PutPointResult putPointResult = geoDataManager.putPoint(putPointRequest);

		printPutPointResult(putPointResult);			
	}
	
	private void printPutPointResult(PutPointResult putPointResult) throws JsonParseException,
	IOException {
		Map<String, String> jsonMap = new HashMap<String, String>();
		jsonMap.put("action", "put-point");
		
		System.out.println(mapper.writeValueAsString(jsonMap));
	}
}
