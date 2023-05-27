package it.uniroma2.sabd.dao;

import it.uniroma2.sabd.engineering.SparkSingleton;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class HdfsDAO {

    public static Dataset<Row> loadDataFrame(String datasetName) throws IOException {
        SparkSession sparkSession = SparkSingleton.getInstance().getSparkSession() ;

        InputStream input = HdfsDAO.class.getClassLoader().getResourceAsStream("hdfs.properties") ;

        Properties hdfsProperties = new Properties() ;
        hdfsProperties.load(input);

        String hdfsHost = hdfsProperties.getProperty("hdfs.host") ;
        String hdfsPort = hdfsProperties.getProperty("hdfs.port") ;

        String hdfsPath = "hdfs://" +
                hdfsHost +
                ":" +
                hdfsPort +
                "/" +
                datasetName ;

        Dataset<Row> dataset = sparkSession.read().csv(hdfsPath);


        return  dataset ;
    }
}
