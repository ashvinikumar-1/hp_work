package mllibSpark;

import java.util.HashMap;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import javassist.bytecode.Descriptor.Iterator;
import scala.Tuple2;

public class HeartDisease {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("heart-disease")
                .master("local[*]")
                .getOrCreate();

        //input path
        String input= "/home/ashvinikumar/Desktop/data/processed.cleveland.data.csv";

        //load csv
        Dataset<Row> my_data =  sparkSession.read().format("csv").option("header", "false")
                .option("inferSchema", true).load(input);

        my_data.show(false);
        RDD<String> linesRDD = sparkSession.sparkContext().textFile(input, 2);

//        Using the replaceAll() method we have handled the invalid values like missing values that are specified
//        in the original file using ? character. To get rid of the missing or invalid value we have replaced them
//        with a very large value that has no side-effect to the original classification or predictive results.
//                The reason behind this is that missing or sparse data can lead you to highly misleading results.
        JavaRDD<LabeledPoint> data = linesRDD.toJavaRDD().map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String row) throws Exception {
                String line = row.replaceAll("\\?", "999999.0");
                String[] tokens = line.split(",");
                Integer last = Integer.parseInt(tokens[13]);
                double[] features = new double[13];
                for (int i = 0; i < 13; i++) {
                    features[i] = Double.parseDouble(tokens[i]);
                }
                Vector v = new DenseVector(features);
                Double value = 0.0;
                if (last.intValue() > 0)
                    value = 1.0;
                LabeledPoint lp = new LabeledPoint(value, v);
                return lp;
            }
        });
        //System.out.println(data.collect());

        double[] weights = {0.7, 0.3};
        long split_seed = 12345L;
        JavaRDD<LabeledPoint>[] split = data.randomSplit(weights, split_seed);
        JavaRDD<LabeledPoint> training = split[0];
        JavaRDD<LabeledPoint> test = split[1];

        final double stepSize = 0.0000000009;
        final int numberOfIterations = 40;
        LinearRegressionModel model = LinearRegressionWithSGD.train(JavaRDD.toRDD(training), numberOfIterations, stepSize);

        String model_storage_loc = "models/heartModel";
        model.save(sparkSession.sparkContext(), model_storage_loc);


        JavaPairRDD<Double, Double> predictionAndLabel =
                test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<>(model.predict(p.features()), p.label());
                    }
                });

        double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Double, Double> pl) {
                return pl._1().equals(pl._2());
            }
        }).count() / (double) test.count();
        System.out.println("Accuracy of the classification: "+accuracy);


    }
}
