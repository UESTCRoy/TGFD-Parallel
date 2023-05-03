package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;


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

    public byte[] serializeObject(Object obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        }
    }

}

