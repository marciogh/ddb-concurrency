package com.marciogh;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

public class DDB {

    static DynamoDbClient client;
    static String tableName = "customerComm";
    static String pendingLSIName = "customerPending";
    static List<String> customers =
            List.of("0e6ed455-c88c-4067-8a4f-9e1b26e5d877", "b1a5b906-447a-494d-9c1b-204d80a419b8",
                    "2c0b1feb-8aab-47e0-b199-7aebcbea5706", "2961b6d0-778f-4b67-b299-e1897b323b20",
                    "a6bab803-7983-4ddf-9fbb-1cfa504bccb2", "0450cd50-e8d5-482f-afd8-73b8308711ea");

    static void createTable() {
        client.createTable(CreateTableRequest.builder().tableName(tableName)
                .billingMode(BillingMode.PROVISIONED)
                .provisionedThroughput(
                        ProvisionedThroughput.builder().readCapacityUnits(Long.valueOf(20))
                                .writeCapacityUnits(Long.valueOf(20)).build())
                .keySchema(
                        KeySchemaElement.builder().keyType(KeyType.HASH).attributeName("customerId")
                                .build(),
                        KeySchemaElement.builder().keyType(KeyType.RANGE).attributeName("nextComm")
                                .build())
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("customerId")
                                .attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("nextComm")
                                .attributeType(ScalarAttributeType.N).build(),
                        AttributeDefinition.builder().attributeName("pendingProcess")
                                .attributeType(ScalarAttributeType.N).build())
                // sparse Index for pending
                .localSecondaryIndexes(
                        LocalSecondaryIndex.builder().indexName(pendingLSIName)
                                .keySchema(
                                        KeySchemaElement.builder().keyType(KeyType.HASH)
                                                .attributeName("customerId").build(),
                                        KeySchemaElement.builder().keyType(KeyType.RANGE)
                                                .attributeName("pendingProcess").build())
                                .projection(Projection.builder().projectionType(ProjectionType.ALL)
                                        .build())
                                .build())
                .build());
    }

    static void putItem(String customerId, long nextComm, boolean pending) {
        Map<String, AttributeValue> item =
                new HashMap<>(Map.of("customerId", AttributeValue.fromS(customerId), "nextComm",
                        AttributeValue.fromN(String.valueOf(nextComm))));
        if (pending) {
            item.put("pendingProcess", AttributeValue.fromN("1"));
        }
        client.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());
    }

    static void putItemsPessimisticLock(Map<AttributeValue, AttributeValue> items) {
        if (items.size() == 0) {
            return;
        }
        Put.Builder putBuilder = Put.builder();
        for (AttributeValue customerId : items.keySet()) {
            putBuilder.item(Map.of("customerId", customerId, "nextComm", items.get(customerId)));
        }
        client.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(
                TransactWriteItem.builder().put(putBuilder.tableName(tableName).build()).build())
                .build());
    }

    static QueryResponse queryPending(String customerId) {
        QueryResponse q = client.query(QueryRequest.builder().tableName(tableName)
                .indexName(pendingLSIName)
                .keyConditionExpression(
                        "customerId = :customerId AND pendingProcess = :pendingProcess")
                .expressionAttributeValues(Map.of(":customerId", AttributeValue.fromS(customerId),
                        ":pendingProcess", AttributeValue.fromN("1")))
                .limit(50).consistentRead(true).build());
        return q;
    }

    static int process(QueryResponse q) {
        int counter = 0;
        long now = LocalDateTime.now().toInstant(ZoneOffset.of("Z")).toEpochMilli();
        for (Map<String, AttributeValue> item : q.items()) {
            if (Long.valueOf(item.get("nextComm").n()) < now) {
                System.out.println("Processing: " + item.get("customerId").s() + ": "
                        + item.get("nextComm").n());
                try {
                    client.updateItem(UpdateItemRequest.builder().tableName(tableName)
                            .key(Map.of("customerId", item.get("customerId"), "nextComm",
                                    item.get("nextComm")))
                            .conditionExpression("attribute_exists(pendingProcess)")
                            .updateExpression("REMOVE pendingProcess").build());
                    counter++;
                } catch (ConditionalCheckFailedException e) {
                    System.out.println("Conditional request failed for "
                            + item.get("customerId").s() + " " + item.get("nextComm").n());
                    return counter;
                }
            }
        }
        return counter;
    }

    static void deleteTable() {
        client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
    }

    public static void main(String[] args) throws InterruptedException {
        client = DynamoDbClient.builder().region(Region.SA_EAST_1).build();

        // createTable();
        // deleteTable();

        Runnable generator = new CommGenerator(customers);
        Runnable consumer = new CommConsumer(customers);


        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < 128; i++) {
            Thread t = new Thread(generator);
            t.start();
            threads.add(t);
        }
        for (int i = 0; i < 4; i++) {
            Thread t = new Thread(consumer);
            t.start();
            threads.add(t);
        }

        for (Thread t : threads) {
            t.join();
        }

        System.out.print("generated: " + CommGenerator.counter);
        System.out.print("consumed: " + CommConsumer.counter);

    }
}
