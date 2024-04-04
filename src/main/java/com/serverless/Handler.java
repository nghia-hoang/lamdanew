package com.serverless;

import java.net.URI;
import java.util.Collections;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

public class Handler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

	private static final Logger LOG = Logger.getLogger(Handler.class);
	
	@Override
	public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
		LOG.info("received: {}" + input);
		Response responseBody = new Response("Go Serverless v1.x! Your function executed successfully!", input);
		System.out.println("Listing your Amazon DynamoDB tables:\n");
//        Region region = Region.US_EAST_1;
//        DynamoDbClient ddb = DynamoDbClient.builder()
//                .region(region)
//                .build();
//        List<String> a = listAllTables(ddb);
//        Map<String, Object> b = new HashMap<>();
//        b.put("a", a);
//        Response responseBody = new Response("Go Serverless v1.x! Your function executed successfully!", responseBody);
//        ddb.close();
		
		try {
            String port = "8000";
            String uri = "localhost:" + port;
//            // Create an in-memory and in-process instance of DynamoDB Local that runs over HTTP
//            final String[] localArgs = {"-inMemory", "-port", port};
//            System.out.println("Starting DynamoDB Local...");
//            server = ServerRunner.createServerFromCommandLineArgs(localArgs);
//            server.start();

            //  Create a client and connect to DynamoDB Local
            //  Note: This is a dummy key and secret and AWS_ACCESS_KEY_ID can contain only letters (A–Z, a–z) and numbers (0–9).
            DynamoDbClient ddbClient = DynamoDbClient.builder()
                    .endpointOverride(URI.create(uri))
                    .httpClient(UrlConnectionHttpClient.builder().build())
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                    .build();

            String tableName = "Music2";
            String keyName = "Artist2";

            // Create a table in DynamoDB Local with table name Music and partition key Artist
            // Understanding core components of DynamoDB: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html
            createTable(ddbClient, tableName, keyName);
		} catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
      }
		return ApiGatewayResponse.builder()
				.setStatusCode(200)
				.setObjectBody(responseBody)
				.setHeaders(Collections.singletonMap("X-Powered-By", "AWS Lambda & serverless"))
				.build();
	}

	   public  String createTable(DynamoDbClient ddb, String tableName, String key) {

	        DynamoDbWaiter dbWaiter = ddb.waiter();
	        CreateTableRequest request = CreateTableRequest.builder()
	                .attributeDefinitions(AttributeDefinition.builder()
	                        .attributeName(key)
	                        .attributeType(ScalarAttributeType.S)
	                        .build())
	                .keySchema(KeySchemaElement.builder()
	                        .attributeName(key)
	                        .keyType(KeyType.HASH)
	                        .build())
	                .provisionedThroughput(ProvisionedThroughput.builder()
	                        .readCapacityUnits(Long.valueOf(5))
	                        .writeCapacityUnits(Long.valueOf(5))
	                        .build())
	                .tableName(tableName)
	                .build();

	        String newTable = "";
	        try {
	            CreateTableResponse response = ddb.createTable(request);
	            DescribeTableRequest tableRequest = DescribeTableRequest.builder()
	                    .tableName(tableName)
	                    .build();

	            // Wait until the Amazon DynamoDB table is created
	            WaiterResponse<DescribeTableResponse> waiterResponse = dbWaiter.waitUntilTableExists(tableRequest);
	            waiterResponse.matched().response().ifPresent(System.out::println);

	            newTable = response.tableDescription().tableName();
	            return newTable;

	        } catch (DynamoDbException e) {
	            System.err.println(e.getMessage());
	            System.exit(1);
	        }
	        return "";
	    }
}
