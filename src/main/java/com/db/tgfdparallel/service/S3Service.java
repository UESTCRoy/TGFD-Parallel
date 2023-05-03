package com.db.tgfdparallel.service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;

@Service
public class S3Service {
    private static final Logger logger = LoggerFactory.getLogger(S3Service.class);
    private final AmazonS3 s3Client;

    public S3Service() {
        s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_2)
                .withCredentials(new ProfileCredentialsProvider())
                .build();
    }

    public void uploadObject(String bucketName, String keyName, Object obj) {
        logger.info("uploadObject: bucketName={}, keyName={}, object={}", bucketName, keyName, obj);
        try {
            byte[] data = serializeObject(obj);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(data.length);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
            PutObjectRequest request = new PutObjectRequest(bucketName, keyName, inputStream, metadata);
            s3Client.putObject(request);
        } catch (AmazonServiceException e) {
            logger.error("Error while uploading object: {}", e.getErrorMessage());
        } catch (IOException e) {
            logger.error("Error while serializing object: {}", e.getMessage());
        }
    }

    public Object downloadObject(String bucketName, String keyName) throws IOException {
        logger.info("downloadObject: bucketName={}, keyName={}", bucketName, keyName);

        Object obj = null;
        try {
            // Get an object and print its contents.
            System.out.println("Downloading an object");
            S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucketName, keyName));
            try (InputStream objectContent = fullObject.getObjectContent()) {
                obj = deserializeObject(objectContent);
            }
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            logger.error("Error while downloading object: {}", e.getErrorMessage());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        return obj;
    }

    public static byte[] serializeObject(Object obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        }
    }

    public static Object deserializeObject(InputStream objectContent) throws IOException, ClassNotFoundException {
        try (ObjectInputStream in = new ObjectInputStream(objectContent)) {
            return in.readObject();
        }
    }
}