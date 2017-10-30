package mllibSpark;


import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import udf.UDFRegister;


public class ZhPredict {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("zh-prediction")
                .master("local[*]")
                .getOrCreate();

        UDFRegister.registerAllCustomUDFS(sparkSession);
        //input path
        String input = "/home/ashvinikumar/Desktop/datazh/zhvalraw.csv";

        //load csv
        Dataset<Row> my_data2 =   sparkSession.read().format("csv").option("header", "true")
                .option("inferSchema", true).load(input);

        my_data2.createOrReplaceTempView("my_data2");

        Dataset<Row> my_data1 = sparkSession.sql("select viewed," +
                "apheight(apheight) as apheight," +
                "CreativeSize,fp,ivp,pos,tile,arc,art from my_data2 ");
        my_data1.show();

        StringIndexer indexer = new StringIndexer()
                .setInputCol("CreativeSize")
                .setOutputCol("CreativeIndex");

        Dataset<Row> indexed1 = indexer.fit(my_data1).transform(my_data1);


        StringIndexer indexer2 = new StringIndexer()
                .setInputCol("art")
                .setOutputCol("artIndex");

        Dataset<Row> indexed2 = indexer2.fit(indexed1).transform(indexed1);
        //indexed2.show();



        indexed2.createOrReplaceTempView("my_data");

        Dataset<Row> my_data = sparkSession.sql("select viewed,CreativeIndex,apheight,fp,ivp,pos,tile,arc,artIndex from my_data");


        //my_data.coalesce(1).write().format("csv").mode("append").save("/home/ashvinikumar/Desktop/data3.csv");

        //define the feature columns to put in the feature vector

        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(new String[]{"CreativeIndex","apheight","fp","ivp","tile","pos","arc","artIndex"})
                .setOutputCol("features");
        Dataset<Row> assembled1 = assembler1.transform(my_data);


        assembled1.show(10,false);

        //  Create a label column with the StringIndexer
        StringIndexerModel indexer3 = new StringIndexer()
                .setInputCol("viewed")
                .setOutputCol("label")
                .fit(assembled1);
        Dataset<Row> indexed = indexer3.transform(assembled1);
        //indexed.show();

        double[] weights = {0.7, 0.3};
        long split_seed = 5043L;
        Dataset<Row>[] split = indexed.randomSplit(weights);
        Dataset<Row> training = split[0];
        Dataset<Row> test = split[1];



        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);


        // Fit the model
        LogisticRegressionModel lrModel = lr.fit(training);
        // Print the coefficients and intercept for logistic regression
        System.out.println("Coefficients: "
                + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

        Dataset<Row> predictions = lrModel.transform(test);
        predictions.show(10000);

        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator().
                setLabelCol("label").
                setRawPredictionCol("rawPrediction")
                .setMetricName("areaUnderROC");

        Double accuracy = evaluator.evaluate(predictions);
        System.out.println("Accuracy="+accuracy);




    }
}
