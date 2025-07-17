
public CompletableFuture<Void> processAsync(String providedEdt, String jobId) {
    return CompletableFuture.supplyAsync(() -> {
        ScanRequest.Builder scanBuilder = ScanRequest.builder()
            .tableName(DYNAMODB_TABLE)
            .filterExpression("edt > :edtVal")
            .expressionAttributeValues(expressionValues)
            .limit(500);

        if (lastKey != null) {
            scanBuilder.exclusiveStartKey(lastKey);
        }

        return dynamoDbClient.scan(scanBuilder.build());
    }).thenApply(response -> {
        int responseSize = response.items().size();
        rowCount += responseSize;
        logger.log(":::: " + responseSize + " items found on batch " + batchNumber);

        return response.items();
    }).thenAcceptAsync(items -> {
        List<CompletableFuture<Void>> futures = items.stream()
            .map(item -> CompletableFuture.runAsync(() -> {
                String s3Key = item.get(s3Key_Label).s();
                String entityType = item.get(partyTypeLabel).s();
                String taxId = item.get("ssn").s();
                String npn = item.get("npn").s();
                String officeCode = item.get("prim_ofcd").s();

                List<String> partyErrorMessages = new ArrayList<>();

                if (PARTY.equalsIgnoreCase(entityType)) {
                    if (taxId != null && !taxId.isEmpty()) {
                        Party party = new Party();
                        party.setTaxId(taxId);
                        parties.getParties().add(party);
                    } else {
                        partyErrorMessages.add("TAXID is NULL");
                    }
                } else {
                    partyErrorMessages.add("Entity type mismatch");
                }

                // Optionally log or collect error messages
            }))
            .collect(Collectors.toList());

        // Wait for all item processing to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();




import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.file.Files;
import java.util.*;

public class NaicsCodeComparator {

    public static void main(String[] args) throws Exception {
        String excelPath = "src/main/resources/Filtered_Naics.xlsx";
        String jsonPath = "src/main/resources/naicscode_list.json";

        Map<String, String> excelMap = loadNaicsFromExcel(excelPath);
        Map<String, String> jsonMap = loadNaicsFromJson(jsonPath);

        Set<String> commonCodes = new HashSet<>(excelMap.keySet());
        commonCodes.retainAll(jsonMap.keySet());

        List<String> exactMatches = new ArrayList<>();
        List<String> mismatchedDescriptions = new ArrayList<>();

        for (String code : commonCodes) {
            String excelDesc = excelMap.get(code).trim();
            String jsonDesc = jsonMap.get(code).trim();
            if (excelDesc.equalsIgnoreCase(jsonDesc)) {
                exactMatches.add(code);
            } else {
                mismatchedDescriptions.add(code + " ➤ Excel: " + excelDesc + " | JSON: " + jsonDesc);
            }
        }

        Set<String> onlyInExcel = new HashSet<>(excelMap.keySet());
        onlyInExcel.removeAll(jsonMap.keySet());

        Set<String> onlyInJson = new HashSet<>(jsonMap.keySet());
        onlyInJson.removeAll(excelMap.keySet());

        // Print summary
        System.out.println("✅ Exact Code+Description Matches: " + exactMatches.size());
        System.out.println("⚠️ Mismatched Descriptions: " + mismatchedDescriptions.size());
        System.out.println("❌ Missing in JSON: " + onlyInExcel);
        System.out.println("❌ Missing in Excel: " + onlyInJson);
        System.out.println("\n🔍 Description Mismatches:");
        mismatchedDescriptions.forEach(System.out::println);
    }

    // Excel loader: returns Map<naicsCode, naicsDescription>
    public static Map<String, String> loadNaicsFromExcel(String filePath) throws IOException {
        Map<String, String> map = new HashMap<>();
        FileInputStream fis = new FileInputStream(filePath);
        Workbook workbook = new XSSFWorkbook(fis);
        Sheet sheet = workbook.getSheetAt(0);

        for (Row row : sheet) {
            Cell codeCell = row.getCell(1); // Column B
            Cell descCell = row.getCell(2); // Column C

            if (codeCell == null || descCell == null) continue;

            String code = codeCell.getCellType() == CellType.NUMERIC
                    ? String.valueOf((long) codeCell.getNumericCellValue())
                    : codeCell.getStringCellValue().trim();

            String desc = descCell.getStringCellValue().trim();
            map.put(code, desc);
        }

        workbook.close();
        return map;
    }

    // JSON loader: returns Map<naicsCode, naicsDescription>
    public static Map<String, String> loadNaicsFromJson(String filePath) throws IOException {
        Map<String, String> map = new HashMap<>();
        String content = new String(Files.readAllBytes(new File(filePath).toPath()));

        JSONObject root = new JSONObject(content);
        JSONObject data = root.getJSONObject("data");
        JSONArray naicsArray = data.getJSONArray("naics");

        for (int i = 0; i < naicsArray.length(); i++) {
            JSONObject obj = naicsArray.getJSONObject(i);
            map.put(obj.getString("naicsCode"), obj.getString("naicsDescription"));
        }

        return map;
    }
}



-----------



public static Set<String> loadNaicsCodesFromJson(String filePath) throws IOException {
    Set<String> codes = new HashSet<>();
    
    String content = new String(Files.readAllBytes(new File(filePath).toPath()));
    
    JSONObject jsonObject = new JSONObject(content);
    JSONObject dataObject = jsonObject.getJSONObject("data"); // "data" key
    JSONArray naicsArray = dataObject.getJSONArray("naics");   // "naics" array
    
    for (int i = 0; i < naicsArray.length(); i++) {
        JSONObject obj = naicsArray.getJSONObject(i);
        codes.add(obj.getString("naicsCode"));
    }
    
    return codes;
}


import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.*;

public class NAICSCodeMatcher {
    public static void main(String[] args) throws Exception {
        // Load Excel NAICS codes
        Set<String> excelCodes = loadNaicsCodesFromExcel("path_to_excel.xlsx");

        // Load JSON NAICS codes
        Set<String> jsonCodes = loadNaicsCodesFromJson("path_to_json.json");

        // Match
        Set<String> common = new HashSet<>(excelCodes);
        common.retainAll(jsonCodes);

        Set<String> inExcelNotInJson = new HashSet<>(excelCodes);
        inExcelNotInJson.removeAll(jsonCodes);

        Set<String> inJsonNotInExcel = new HashSet<>(jsonCodes);
        inJsonNotInExcel.removeAll(excelCodes);

        // Print results
        System.out.println("✅ Common Codes: " + common);
        System.out.println("❌ Missing in JSON: " + inExcelNotInJson);
        System.out.println("❌ Missing in Excel: " + inJsonNotInExcel);
    }

    private static Set<String> loadNaicsCodesFromExcel(String filePath) throws Exception {
        Set<String> codes = new HashSet<>();
        FileInputStream fis = new FileInputStream(filePath);
        Workbook workbook = new XSSFWorkbook(fis);
        Sheet sheet = workbook.getSheetAt(0);

        for (Row row : sheet) {
            Cell cell = row.getCell(1); // Column B
            if (cell != null && cell.getCellType() == CellType.NUMERIC) {
                codes.add(String.valueOf((long) cell.getNumericCellValue()));
            } else if (cell != null && cell.getCellType() == CellType.STRING) {
                codes.add(cell.getStringCellValue().trim());
            }
        }
        workbook.close();
        return codes;
    }

    private static Set<String> loadNaicsCodesFromJson(String filePath) throws Exception {
        Set<String> codes = new HashSet<>();
        StringBuilder sb = new StringBuilder();
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line;

        while ((line = br.readLine()) != null) {
            sb.append(line);
        }
        br.close();

        JSONArray jsonArray = new JSONArray(sb.toString());
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = jsonArray.getJSONObject(i);
            codes.add(obj.getString("naicsCode"));
        }
        return codes;
    }
}
