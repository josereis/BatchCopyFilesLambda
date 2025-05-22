//package com.josereis;
//
//import com.amazonaws.auth.AWSStaticCredentialsProvider;
//import com.amazonaws.auth.BasicAWSCredentials;
//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3ClientBuilder;
//import com.amazonaws.services.s3.model.CopyObjectRequest;
//import com.amazonaws.services.s3.model.S3Object;
//import com.amazonaws.services.s3.model.S3ObjectInputStream;
//
//import java.io.BufferedReader;
//import java.io.InputStreamReader;
//import java.time.Duration;
//import java.time.Instant;
//
//public class Main {
//    public static void main(String[] args) {
//        Instant start = Instant.now();
//
//        String region = System.getenv("AWS_REGION");
//        String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
//        String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
//
//        BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
//        AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
//                .withCredentials(new AWSStaticCredentialsProvider(credentials))
//                .withRegion(region)
//                .build();
//
//        String bucket = "bucket-name", filePath = "path-file-reference-to-source-and-target.csv";
//        boolean readHeader = false;
//        int linhasLidas = 0, copiasRealizadas = 0;
//        try {
//            S3Object s3object = amazonS3.getObject(bucket, filePath);
//            S3ObjectInputStream inputStream = s3object.getObjectContent();
//            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
//
//            String row;
//            while ((row = reader.readLine()) != null) {
//                if(readHeader) {
//                    String[] parts = row.split(",");
//
//                    String documentoId = parts[0].replaceAll("^\"+|\"+$", "").trim();
//                    String source = parts[2].replaceAll("^\"+|\"+$", "").trim();
//                    String target = parts[3].replaceAll("^\"+|\"+$", "").trim();
//
//                    try {
//                        CopyObjectRequest copyReq = new CopyObjectRequest(bucket, source, bucket, target);
//                        amazonS3.copyObject(copyReq);
//
//                        System.out.println(String.format("Document copied from %s to %s", source, target));
//                        copiasRealizadas++;
//                    } catch (Exception e) {
//                        System.out.println(String.format("Error copying document %s to %s", source, target));
//                    }
//                    linhasLidas++;
//                } else {
//                    readHeader = true;
//                }
//            }
//            Instant end = Instant.now();
//
//            System.out.println(String.format("%d linhas processadas | %d arquivos copiados | duração de %d min", linhasLidas, copiasRealizadas, Duration.between(start, end).toMinutes()));
//        } catch (Exception e) {
//            Instant end = Instant.now();
//            System.out.println("Erro ao processar: " + e.getMessage());
//            System.out.println(String.format("%d linhas processadas | %d arquivos copiados | duração de %d min", linhasLidas, copiasRealizadas, Duration.between(start, end).toMinutes()));
//        }
//    }
//}