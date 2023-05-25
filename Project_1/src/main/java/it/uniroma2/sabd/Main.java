package it.uniroma2.sabd;

import it.uniroma2.sabd.engineering.SparkSingleton;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {

        SparkContext sparkContext = SparkSingleton.getInstance().getSparkContext() ;

        SparkSession sparkSession = SparkSingleton.getInstance().getSparkSession() ;
    }
}