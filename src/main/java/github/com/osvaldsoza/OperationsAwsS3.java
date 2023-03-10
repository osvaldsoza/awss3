package github.com.osvaldsoza;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

public class OperationsAwsS3 {

    private final AmazonS3 clientAmazonS3;

    public OperationsAwsS3(String accessKey, String secretKey) {
        var credentials = new BasicAWSCredentials(accessKey, secretKey);
        clientAmazonS3 = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.US_EAST_1)
                .build();
    }

    public void createBucket(final String nameBucket) {
        if (!clientAmazonS3.doesBucketExistV2(nameBucket))
            clientAmazonS3.createBucket(nameBucket);
        else {
            System.out.println("Name of the bucket [" + nameBucket + "] already been used");
            return;
        }
    }

    public List<String> listBuckets() {
        return clientAmazonS3.listBuckets()
                .stream()
                .map(bucket -> bucket.getName())
                .collect(Collectors.toList());
    }

    public void deleteBucket(final String nameBucket) {
        if (clientAmazonS3.doesBucketExistV2(nameBucket))
            clientAmazonS3.deleteBucket(nameBucket);
        else {
            System.out.println("Name of the bucket [" + nameBucket + "] already been used");
            return;
        }
    }

    public void sendFile(String nameBucket, String destinationFile, String originFile) {
        if (clientAmazonS3.doesBucketExistV2(nameBucket)){
            clientAmazonS3.putObject(nameBucket, destinationFile, new File(originFile));
        }else{
            System.out.println("The bucket [" + nameBucket + "] not exists");
            return;

        }
    }

    public List<String> listFiles(String nameBucket) {
        var listObjects = clientAmazonS3.listObjects(nameBucket);
        return listObjects.getObjectSummaries()
                .stream()
                .map(object -> object.getKey())
                .collect(Collectors.toList());
    }

    public void deleteFile(String nameBucket, String keyFile){

        if (clientAmazonS3.doesBucketExistV2(nameBucket))
        clientAmazonS3.deleteObject(nameBucket,keyFile);
        else {
            System.out.println("The bucket [" + nameBucket + "] already exists");
            return;
        }
    }

    public static void handleBucket(){
        var operacoes = new OperationsAwsS3(Credentials.ACCESS_KEY, Credentials.SECRET_KEY);
        var nameBucket = "osf77";
     //   operacoes.createBucket(nameBucket);
        operacoes.listBuckets().forEach(System.out::println);
       operacoes.deleteBucket(nameBucket);

        operacoes.listBuckets().forEach(System.out::println);
    }

    public static void handleFile(){
        var operacoes = new OperationsAwsS3(Credentials.ACCESS_KEY, Credentials.SECRET_KEY);
        var nameBucket = "osf77";
        var originFile = "/Users/osvaldoferreira/Downloads/Musica/Partituras/Brejeiro.pdf";
        var destinationFile = "Brejeiro.pdf";
    //    operacoes.sendFile(nameBucket,destinationFile,originFile);
        operacoes.listFiles(nameBucket).forEach(System.out::println);
        operacoes.deleteFile(nameBucket,destinationFile);
       operacoes.listFiles(nameBucket).forEach(System.out::println);
    }
}
