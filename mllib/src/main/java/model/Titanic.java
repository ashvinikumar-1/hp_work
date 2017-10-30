package model;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.mean;

public class Titanic {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("titanic survivour")
                .master("local[2]")
                .getOrCreate();


        String trainPath = "/home/ashvinikumar/Desktop/data/titanic/train.csv";
        String testPath = "/home/ashvinikumar/Desktop/data/titanic/test.csv";


        Dataset<Row> trainData = sparkSession.read().format("csv").option("header","true").option("inferSchema",true).load(trainPath);
        Dataset<Row> testData = sparkSession.read().format("csv").option("header",true).option("inferSchema",true).load(testPath);


        //trainData.na().drop();

        double avgAge = ((double) trainData.select(mean("Age")).first().get(0));
        Dataset<Row> newTrain = trainData.na().fill(avgAge).na().drop(new String[]{"Cabin","Embarked"});
        newTrain.show(1000);


    }
}