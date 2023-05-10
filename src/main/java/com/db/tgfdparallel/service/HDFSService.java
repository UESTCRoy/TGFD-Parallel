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
        try {
            byte[] data = serializeObject(obj);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(data);

            FileSystem fs = FileSystem.get(conf);

            Path directoryPath = new Path(directoryName);
            if (!fs.exists(directoryPath)) {
                fs.mkdirs(directoryPath);
            }

            Path filePath = new Path(directoryPath, fileName);
            try (FSDataOutputStream out = fs.create(filePath)) {
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) > 0) {
                    out.write(buffer, 0, bytesRead);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Object downloadObject(String directoryName, String fileName) {
        Object obj = null;
        try {
            FileSystem fileSystem = FileSystem.get(conf);

            FSDataInputStream inputStream = fileSystem.open(new Path(directoryName, fileName));
            try (ObjectInputStream in = new ObjectInputStream(inputStream)) {
                obj = in.readObject();
            } catch (IOException e) {
                logger.error("Error while reading the object from HDFS: " + e.getMessage());
                return null;
            } catch (ClassNotFoundException e) {
                logger.error("Error while deserializing the object from HDFS: " + e.getMessage());
                return null;
            }

            inputStream.close();
        } catch (IOException e) {
            logger.error("Error while accessing HDFS: " + e.getMessage());
            return null;
        }
        return obj;
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
            System.out.println(e.getMessage());
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

