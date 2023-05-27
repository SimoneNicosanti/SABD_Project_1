package it.uniroma2.sabd.controller;

import it.uniroma2.sabd.dao.HdfsDAO;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

public class Controller {


    public static void control() throws IOException {

        Dataset<Row> dataset = HdfsDAO.loadDataFrame("Dataset.csv") ;

    }
}
