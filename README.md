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
