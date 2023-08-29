package com.db.tgfdparallel.service;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import com.db.tgfdparallel.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.*;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;

import java.io.*;
import java.nio.file.Paths;

@Service
public class S3Service {
    private static final Logger logger = LoggerFactory.getLogger(S3Service.class);
    private final S3Client s3Client;
    private final AppConfig config;
    private final S3TransferManager transferManager;
    private final String bucketName = "tgfd";
    private final String S3TempPrefix = "temp/";

    public S3Service(AppConfig config) {
        s3Client = S3Client.builder()
                .region(Region.US_EAST_2)
                .build();
        this.config = config;
        this.transferManager = createCustomTm();
    }

    private static S3TransferManager createCustomTm() {
        S3AsyncClient s3AsyncClient =
                S3AsyncClient.crtBuilder()
                        .region(Region.US_EAST_2)
                        .build();

        return S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build();
    }

    public void uploadObject(String keyName, Object obj) {
        logger.info("uploadObject: bucketName={}, keyName={}", bucketName, keyName);
        String awsPath = S3TempPrefix + keyName;

        try {
            byte[] data = serializeObject(obj);
            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(awsPath)
                    .build();
            s3Client.putObject(putOb, RequestBody.fromBytes(data));
        } catch (S3Exception e) {
            logger.error("Error while uploading object: {}", e.getMessage());
        } catch (IOException e) {
            logger.error("Error while serializing object: {}", e.getMessage());
        }
    }

    public Object downloadObject(String keyName) throws IOException {
        logger.info("downloadObject: bucketName={}, keyName={}", bucketName, keyName);
        String awsPath = S3TempPrefix + keyName;
        Object obj = null;
        try {
            logger.info("Downloading an object");
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(awsPath)
                    .bucket(bucketName)
                    .build();
            ResponseInputStream<GetObjectResponse> object = s3Client.getObject(objectRequest);
            obj = deserializeObject(object);

        } catch (S3Exception e) {
            logger.error("Error while downloading object: {}", e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        return obj;
    }

    public void uploadWholeTextFile(String key, String textToBeUploaded) {
        logger.info("Uploading to Amazon S3");
        String awsPath = S3TempPrefix + key;

        try {
            PutObjectRequest objectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(awsPath)
                    .build();
            s3Client.putObject(objectRequest, RequestBody.fromString(textToBeUploaded));
            logger.info("Uploading Done. [Bucket name: " + bucketName + "] [Key: " + key + "]");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public StringBuilder downloadWholeTextFile(String key) {
        StringBuilder sb = new StringBuilder();
        String awsPath = S3TempPrefix + key;

        try {
            logger.info("Downloading text file from Amazon S3 - Bucket name: " + bucketName + " - Key: " + key);
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(awsPath)
                    .bucket(bucketName)
                    .build();
            ResponseInputStream<GetObjectResponse> objectContent = s3Client.getObject(objectRequest);

            try (BufferedReader br = new BufferedReader(new InputStreamReader(objectContent))) {
                String line;
                while ((line = br.readLine()) != null) {
                    sb.append(line).append("\n");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return sb;
    }

    public void downloadFileToInstance(String key, String downloadedFileWithPath) {
        DownloadFileRequest downloadFileRequest = DownloadFileRequest.builder()
                .getObjectRequest(b -> b.bucket(bucketName).key(key))
                .addTransferListener(LoggingTransferListener.create())
                .destination(Paths.get(downloadedFileWithPath))
                .build();

        FileDownload downloadFile = transferManager.downloadFile(downloadFileRequest);

        CompletedFileDownload downloadResult = downloadFile.completionFuture().join();
        logger.info("Content length [{}]", downloadResult.response().contentLength());
//        return downloadResult.response().contentLength();
    }

    public void downloadObjectsToInstanceDirectory(String destinationPath) {
        String prefix = config.getChangeFilePath();
        DirectoryDownload directoryDownload =
                transferManager.downloadDirectory(DownloadDirectoryRequest.builder()
                        .destination(Paths.get(destinationPath))
                        .listObjectsV2RequestTransformer(l -> l.prefix(prefix))
                        .bucket(bucketName)
                        .build());
        CompletedDirectoryDownload completedDirectoryDownload = directoryDownload.completionFuture().join();

        completedDirectoryDownload.failedTransfers().forEach(fail ->
                logger.warn("Object [{}] failed to transfer", fail.toString()));
//        return completedDirectoryDownload.failedTransfers().size();
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