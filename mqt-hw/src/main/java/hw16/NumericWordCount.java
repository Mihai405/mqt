package hw16;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class NumericWordCount {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("NumericValueCount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        String filePath = "/Users/mihai/Desktop/mqt/mqt/mqt-hw/src/main/java/hw16/numeric_count.txt";
        JavaRDD<String> textFile = sc.textFile(filePath);
        textFile
                .flatMap(line -> Arrays.asList(line.split("[\\s,;]+|(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)")).iterator())
                .filter(NumericWordCount::isNumeric)
                .foreach(value -> System.out.println("Numeric value: " + value));
        sc.stop();
    }

    private static boolean isNumeric(String value) {
        try {
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
