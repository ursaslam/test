import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

import java.util.*;
import java.util.stream.Collectors;

public class DynamoDBFetcher {

    private static final String TABLE_NAME = "YourTableName";
    private static final String TAX_ID = "taxId";

    public static void main(String[] args) {
        Set<String> taxIdSet = new HashSet<>(Arrays.asList("12345", "67890", "ABC123"));

        DynamoDbClient dynamoDb = DynamoDbClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(ProfileCredentialsProvider.create()) // adjust as needed
                .build();

        List<Map<String, AttributeValue>> results = fetchByTaxIds(dynamoDb, taxIdSet);

        // Print results
        for (Map<String, AttributeValue> item : results) {
            System.out.println(item);
        }

        dynamoDb.close();
    }

    public static List<Map<String, AttributeValue>> fetchByTaxIds(DynamoDbClient dynamoDb, Set<String> taxIdSet) {
        List<Map<String, AttributeValue>> resultItems = new ArrayList<>();

        // DynamoDB allows a max of 100 keys per BatchGetItem request
        List<List<String>> chunks = new ArrayList<>(new ArrayList<>(taxIdSet)
                .stream()
                .collect(Collectors.groupingBy(s -> taxIdSet.stream().toList().indexOf(s) / 100))
                .values());

        for (List<String> chunk : chunks) {
            List<Map<String, AttributeValue>> keys = chunk.stream()
                    .map(taxId -> Map.of(TAX_ID, AttributeValue.builder().s(taxId).build()))
                    .collect(Collectors.toList());

            Map<String, KeysAndAttributes> requestItems = Map.of(
                    TABLE_NAME,
                    KeysAndAttributes.builder()
                            .keys(keys)
                            .build()
            );

            BatchGetItemRequest request = BatchGetItemRequest.builder()
                    .requestItems(requestItems)
                    .build();

            BatchGetItemResponse response = dynamoDb.batchGetItem(request);
            resultItems.addAll(response.responses().get(TABLE_NAME));
        }

        return resultItems;
    }
}

 public static void main(String[] args) {
        Set<String> taxIdSet = new HashSet<>(Arrays.asList("12345", "67890", "ABC123"));

        try (DynamoDbClient dynamoDb = DynamoDbClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(ProfileCredentialsProvider.create()) // Use default or other provider as needed
                .build()) {

            List<Map<String, AttributeValue>> results = fetchByTaxIds(dynamoDb, taxIdSet);

            for (Map<String, AttributeValue> item : results) {
                System.out.println(item);
            }

        } // Auto-closes the client safely here
    }

    public static List<Map<String, AttributeValue>> fetchByTaxIds(DynamoDbClient dynamoDb, Set<String> taxIdSet) {
        List<Map<String, AttributeValue>> resultItems = new ArrayList<>();

        // Chunk the tax IDs into groups of up to 100 (DynamoDB BatchGetItem limit)
        List<String> taxIdList = new ArrayList<>(taxIdSet);
        for (int i = 0; i < taxIdList.size(); i += 100) {
            int end = Math.min(i + 100, taxIdList.size());
            List<String> chunk = taxIdList.subList(i, end);

        List<Map<String, AttributeValue>> keys = chunk.stream()
                    .map(taxId -> {
                        Map<String, AttributeValue> keyMap = new HashMap<>();
                        keyMap.put(TAX_ID, AttributeValue.builder().s(taxId).build());
                        return keyMap;
                    })
                    .collect(Collectors.toList());

            Map<String, KeysAndAttributes> requestItems = Map.of(
                    TABLE_NAME,
                    KeysAndAttributes.builder().keys(keys).build()
            );

            BatchGetItemRequest request = BatchGetItemRequest.builder()
                    .requestItems(requestItems)
                    .build();

            BatchGetItemResponse response = dynamoDb.batchGetItem(request);
            resultItems.addAll(response.responses().getOrDefault(TABLE_NAME, List.of()));
        }

        return resultItems;
    }
