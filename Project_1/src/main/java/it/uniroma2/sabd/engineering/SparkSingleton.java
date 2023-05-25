package it.uniroma2.sabd.engineering;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SparkSingleton {

    private final SparkContext sparkContext ;
    private final SparkSession sparkSession ;
    private static SparkSingleton instance;

    private SparkSingleton()  {

        try (InputStream input = getClass().getClassLoader().getResourceAsStream("spark.properties")) {

            Properties sparkProperties = new Properties() ;
            sparkProperties.load(input);

            SparkConf conf = new SparkConf()
                    .setMaster(sparkProperties.getProperty("spark.master"))
                    .setAppName(sparkProperties.getProperty("spark.appName")) ;

            sparkContext = new SparkContext(conf);
            sparkSession = SparkSession.builder()
                    .config(conf)
                    .getOrCreate() ;
        }

        catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static SparkSingleton getInstance() {
        if (instance == null) {
            instance = new SparkSingleton() ;
        }
        return instance;
    }

    public SparkContext getSparkContext() {
        return this.sparkContext ;
    }

    public SparkSession getSparkSession() {
        return this.sparkSession ;
    }
}
