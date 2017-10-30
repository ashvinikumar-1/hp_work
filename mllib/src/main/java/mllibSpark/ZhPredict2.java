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


public class ZhPredict2 {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("zh-prediction")
                .master("local[*]")
                .getOrCreate();

        UDFRegister.registerAllCustomUDFS(sparkSession);
        //input path
        String input = "/home/ashvinikumar/Desktop/datazh/validateWoe.csv";

        //load csv
        Dataset<Row> my_data2 =   sparkSession.read().format("csv").option("header", "true")
                .option("inferSchema", true).load(input).limit(2000);


        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(new String[]{"CreativeSize","DeviceCategory","bsheight","bsWidth","apheight","apWidth","rpheight","rpWidth","fp","ivp","tile",
                        "pos","arc","art"})
                .setOutputCol("features");
        Dataset<Row> assembled1 = assembler1.transform(my_data2);

        assembled1.show();


        //  Create a label column with the StringIndexer
        StringIndexerModel indexer3 = new StringIndexer()
                .setInputCol("viewed")
                .setOutputCol("label")
                .fit(assembled1);

        Dataset<Row> indexed = indexer3.transform(assembled1);
        indexed.show();

        double[] weights = {0.7, 0.3};
        long split_seed = 5043L;
        Dataset<Row>[] split = indexed.randomSplit(weights,split_seed);
        Dataset<Row> training = split[0];
        Dataset<Row> test = split[1];



//        LinearSVC lsvc = new LinearSVC()
//                .setMaxIter(10)
//                .setRegParam(0.1);
//
//        // Fit the model
//        LinearSVCModel lsvcModel = lsvc.fit(training);

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
        //predictions.show(10000);

        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator().
                setLabelCol("label").
                setRawPredictionCol("rawPrediction")
                .setMetricName("areaUnderROC");

        Double accuracy = evaluator.evaluate(predictions);
        System.out.println("Accuracy="+accuracy);




    }
}
