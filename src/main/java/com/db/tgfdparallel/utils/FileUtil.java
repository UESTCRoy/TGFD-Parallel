package com.db.tgfdparallel.utils;

import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileUtil {
    public static JSONArray readJsonFile(String filePath) {
        try {
            String fileContent = new String(Files.readAllBytes(Paths.get(filePath)));
            return new JSONArray(fileContent);
        } catch (IOException | JSONException e) {
            System.err.println("Error reading JSON file: " + e.getMessage());
            return null;
        }
    }
}
