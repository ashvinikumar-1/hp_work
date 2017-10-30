package sparkJob;

import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import udf.UDFRegister;

public class OneHotEncoding {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
            .appName("zh-prediction")
            .master("local[*]")
            .getOrCreate();

        UDFRegister.registerAllCustomUDFS(sparkSession);
    //input path
    String input[] = {"/home/ashvinikumar/Desktop/datazh/zhvalraw.csv","/home/ashvinikumar/Desktop/datazh/zhFNormal.csv/normalSample.csv","/home/ashvinikumar/Desktop/data2/data1.csv"};

    //load csv
    Dataset<Row> df =   sparkSession.read().format("csv").option("header", "true")
            .option("inferSchema", true).load(input[1]);

        StringIndexerModel indexer = new StringIndexer()
                .setInputCol("CreativeSize")
                .setOutputCol("CreativeSizeIndex")
                .fit(df);
        Dataset<Row> indexed = indexer.transform(df);

        OneHotEncoder encoder = new OneHotEncoder()
                .setInputCol("CreativeSizeIndex")
                .setOutputCol("categoryVec");

        Dataset<Row> encoded = encoder.transform(indexed);
        encoded.show();

 }
}
