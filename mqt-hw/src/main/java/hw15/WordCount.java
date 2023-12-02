package hw15;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        String filePath = "/Users/mihai/Desktop/mqt/mqt/mqt-hw/src/main/java/hw15/word_count.txt";
        JavaRDD<String> textFile = sc.textFile(filePath);
        long count = textFile
                .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                .filter(word -> word.equals("master"))
                .count();
        System.out.println("Occurrences of 'master': " + count);
        sc.stop();
    }
}
