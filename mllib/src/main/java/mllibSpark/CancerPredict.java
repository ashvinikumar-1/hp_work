package mllibSpark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CancerPredict {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("cancer-disease")
                .master("local[*]")
                .getOrCreate();

        //input path
        String input= "/home/ashvinikumar/Desktop/data/cancer.csv";





        StructType schema = new StructType(new StructField[]{
                new StructField("id",DataTypes.DoubleType,false,Metadata.empty()),
                new StructField("thickness", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("size",DataTypes.DoubleType,false,Metadata.empty()),
                new StructField("shape",DataTypes.DoubleType,false,Metadata.empty()),
                new StructField("madh", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("epsize", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("bnuc", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("bChrom", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("nNuc", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("mit", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("clas", DataTypes.DoubleType, false, Metadata.empty())

        });

        //load csv
        Dataset<Row> my_data =  sparkSession.read().format("csv").schema(schema).option("header", "false")
                .option("inferSchema", true).load(input);





//        sparkSession.read().format("csv").schema(schema).option("header", "true")
//                .option("delimiter", ",").load(csv_Paths_To_Read[1]);

        //my_data.show(false);
        my_data.createOrReplaceTempView("newData");


        Dataset<Row> newData = sparkSession.sql("select if(clas==4.0,1,0) as clas,thickness,size,shape,madh,epsize,bnuc,bChrom,nNuc,mit " +
                " from newData ").filter("bnuc!=5000");
        //newData.show(1000);


        //define the feature columns to put in the feature vector

        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(new String[]{"thickness","size","shape","madh","epsize","bnuc","bChrom","nNuc","mit"})
                .setOutputCol("features");
        Dataset<Row> assembled1 = assembler1.transform(newData);


        assembled1.show(10,false);

        //  Create a label column with the StringIndexer
        StringIndexerModel indexer = new StringIndexer()
                .setInputCol("clas")
                .setOutputCol("label")
                .fit(assembled1);
        Dataset<Row> indexed = indexer.transform(assembled1);
        indexed.show();

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
        //predictions.show();

        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator().
                setLabelCol("label").
                setRawPredictionCol("rawPrediction")
                .setMetricName("areaUnderROC");

        Double accuracy = evaluator.evaluate(predictions);
        System.out.println("Accuracy="+accuracy);








    }
}
