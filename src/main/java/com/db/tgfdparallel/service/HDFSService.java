package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import org.apache.hadoop.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.nio.charset.StandardCharsets;


@Service
public class HDFSService {
    private static final Logger logger = LoggerFactory.getLogger(HDFSService.class);
    private final Configuration conf;

    @Autowired
    public HDFSService(AppConfig config) {
        conf = new Configuration();
        conf.set(config.getHDFSName(), config.getHDFSAddress());
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    public void uploadObject(String directoryName, String fileName, Object obj) {
        if (directoryName == null || fileName == null || obj == null) {
            logger.error("Invalid input parameters. Directory name, file name, and object must not be null.");
            return;
        }

        FileSystem fs = null;
        try {
            byte[] data = serializeObject(obj);
            if (data == null || data.length == 0) {
                logger.error("Serialization failed or serialized object is empty.");
                return;
            }

            fs = FileSystem.get(conf);

            Path directoryPath = new Path(directoryName);
            if (!fs.exists(directoryPath)) {
                fs.mkdirs(directoryPath);
                logger.info("Created directory: {}", directoryPath);
            }

            Path filePath = new Path(directoryPath, fileName);
            if (fs.exists(filePath)) {
                logger.info("File {} already exists. It will be overwritten.", filePath);
            }

            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
                 FSDataOutputStream out = fs.create(filePath, true)) {
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) > 0) {
                    out.write(buffer, 0, bytesRead);
                }
                logger.info("Successfully uploaded to {}", filePath);
            }
        } catch (IOException e) {
            logger.error("Failed to upload object to {}/{}.", directoryName, fileName, e);
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    logger.error("Failed to close FileSystem object", e);
                }
            }
        }
    }

    public Object downloadObject(String directoryName, String fileName) {
        Object obj = null;
        try {
            FileSystem fileSystem = FileSystem.get(conf);

            Path filePath = new Path(directoryName, fileName);
            if (!fileSystem.exists(filePath)) {
                logger.error("File does not exist: {}/{}", directoryName, fileName);
                return null;
            }

            logger.info("File exists, attempting to open: {}/{}", directoryName, fileName);
            FSDataInputStream inputStream = fileSystem.open(filePath);
            logger.info("Successfully opened the file: {}/{}", directoryName, fileName);
            obj = deserializeObject(inputStream);
        } catch (IOException e) {
            logger.error("Error while accessing HDFS", e);
            return null;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return obj;
    }

    public static Object deserializeObject(InputStream objectContent) throws IOException, ClassNotFoundException {
        try (ObjectInputStream in = new ObjectInputStream(objectContent)) {
            return in.readObject();
        }
    }

    public StringBuilder downloadWholeTextFile(String directoryName, String fileName) {
        StringBuilder sb = new StringBuilder();
        try {

            try (FileSystem fileSystem = FileSystem.get(conf)) {
                Path hdfsReadPath = new Path(directoryName + fileName);
                try (FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
                     BufferedReader bufferedReader = new BufferedReader(
                             new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        sb.append(line).append("\n");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb;
    }

    public void uploadWholeTextFile(String directoryName, String fileName, String textToBeUploaded) {
        try {

            try (FileSystem fileSystem = FileSystem.get(conf)) {
                Path hdfsWritePath = new Path(directoryName + fileName);
                try (FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath, true);
                     BufferedWriter bufferedWriter = new BufferedWriter(
                             new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8))) {
                    bufferedWriter.write(textToBeUploaded);
                    bufferedWriter.newLine();
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public byte[] serializeObject(Object obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        }
    }

}

