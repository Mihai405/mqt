package hw17;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class RemoveWords {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("RemoveWords").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String firstFilePath = "/Users/mihai/Desktop/mqt/mqt/mqt-hw/src/main/java/hw17/first_file.txt";
        JavaRDD<String> firstTextFile = sc.textFile(firstFilePath);
        String secondFilePath = "/Users/mihai/Desktop/mqt/mqt/mqt-hw/src/main/java/hw17/second_file.txt";
        JavaRDD<String> secondTextFile = sc.textFile(secondFilePath);

        JavaRDD<String> wordsToBeRemoved = secondTextFile
                .flatMap(line -> Arrays.asList(line.split("[\\s,;]")).iterator())
                .distinct();

        JavaRDD<String> filteredWords = firstTextFile
                .flatMap(line -> Arrays.asList(line.split("[\\s,;]")).iterator())
                .subtract(wordsToBeRemoved);

        String outputPath = "/Users/mihai/Desktop/mqt/mqt/mqt-hw/src/main/java/hw17/output.txt";
        filteredWords.coalesce(1).saveAsTextFile(outputPath);

        sc.stop();
    }
}
