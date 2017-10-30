package mllibSpark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import udf.UDFRegister;

public class DataGeneration {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("zh-prediction")
                .master("local[*]")
                .getOrCreate();

        UDFRegister.registerAllCustomUDFS(sparkSession);
        //input path
        String input ="/home/ashvinikumar/Desktop/data2/data1.csv";

        //load csv
        Dataset<Row> my_data2 = sparkSession.read().format("csv").option("header", "true")
                .option("inferSchema", true).load(input);

        my_data2.createOrReplaceTempView("my_data2");

        Dataset<Row> my_data1 = sparkSession.sql("select CreativeSize(CreativeSize) as CreativeSize," +
                "DeviceCategory(DeviceCategory) as DeviceCategory,bsheight(bsheight) as bsheight," +
                "bsWidth(bsWidth) as bsWidth,apheight(apheight) as apheight,apWidth(apWidth) as apWidth,rpheight(rpheight) as rpheight," +
                "rpWidth(rpWidth) as rpWidth,fp(fp) as fp,ivp(ivp) as ivp,tile(tile) as tile," +
                "pos(pos) as pos,arc(arc) as arc,art(art) as art,viewed from my_data2");
        //my_data1.show();
        my_data1.coalesce(1).write().mode("append").option("header",true).format("csv").save("/home/ashvinikumar/Desktop/datazh/zhFNormal2.csv");

    }
}