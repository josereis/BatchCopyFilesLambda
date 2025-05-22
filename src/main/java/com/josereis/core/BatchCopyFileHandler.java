package com.josereis.core;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

public class BatchCopyFileHandler implements RequestHandler<Map<String, String>, String> {

    @Override
    public String handleRequest(Map<String, String> input, Context context) {
        String region = System.getenv("AWS_REGION");
        String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");

        BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(region)
                .build();

        try {
            String sourceBucket = input.get("source-bucket"), targetBucket = input.get("target-bucket"), filePath = input.get("csvFile");

            S3Object s3object = amazonS3.getObject(sourceBucket, filePath);
            S3ObjectInputStream inputStream = s3object.getObjectContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String row;
            boolean readHeader = false;
            int linhasLidas = 0, copiasRealizadas = 0;
            while ((row = reader.readLine()) != null) {
                if(readHeader) {
                    String[] parts = row.split(",");

                    String documentoId = parts[0].replaceAll("^\"+|\"+$", "").trim();
                    String source = parts[2].replaceAll("^\"+|\"+$", "").trim();
                    String target = parts[3].replaceAll("^\"+|\"+$", "").trim();

                    try {
                        CopyObjectRequest copyReq = new CopyObjectRequest(sourceBucket, source, targetBucket, target);
                        amazonS3.copyObject(copyReq);

                        context.getLogger().log(String.format("Document copied from %s to %s", source, target));
                        copiasRealizadas++;
                    } catch (Exception e) {
                        context.getLogger().log(String.format("Error copying document %s to %s", source, target));
                    }
                    linhasLidas++;
                } else {
                    readHeader = true;
                }
            }

            return String.format("%d linhas processadas | %d arquivos copiados", linhasLidas, copiasRealizadas);
        } catch (Exception e) {
            context.getLogger().log("Erro ao processar: " + e.getMessage());
            return "Erro: " + e.getMessage();
        }
    }
}
