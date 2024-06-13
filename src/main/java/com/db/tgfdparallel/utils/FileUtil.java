package com.db.tgfdparallel.utils;

import com.db.tgfdparallel.domain.TGFD;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    public static <T> List<T> castList(Object obj, Class<T> clazz) {
        List<T> result = new ArrayList<T>();
        if (obj instanceof List<?>) {
            for (Object o : (List<?>) obj) {
                result.add(clazz.cast(o));
            }
            return result;
        }
        return null;
    }

    public static void saveConstantTGFDsToFile(Map<Integer, List<TGFD>> data, String filename) {
        try (PrintWriter out = new PrintWriter(new FileWriter(filename))) {
            for (Map.Entry<Integer, List<TGFD>> entry : data.entrySet()) {
                out.println("Value: ");
                for (TGFD tgfd : entry.getValue()) {
                    out.println(tgfd.toString());
                }
                out.println();
            }
        } catch (IOException e) {
            System.out.println("An error occurred while writing to file: " + e.getMessage());
            e.printStackTrace();
        }
    }

//    public static String hashString(String input) {
//        try {
//            MessageDigest md = MessageDigest.getInstance("MD5");
//            byte[] hashBytes = md.digest(input.getBytes());
//            return bytesToHex(hashBytes);
//        } catch (NoSuchAlgorithmException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    private static String bytesToHex(byte[] bytes) {
//        StringBuilder hexString = new StringBuilder(2 * bytes.length);
//        for (byte b : bytes) {
//            String hex = Integer.toHexString(0xff & b);
//            if (hex.length() == 1) {
//                hexString.append('0');
//            }
//            hexString.append(hex);
//        }
//        return hexString.toString();
//    }
}
