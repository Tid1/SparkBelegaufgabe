import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Longest Word")
                .setMaster("local")
                .set("spark.executor.cores", "8");

        JavaSparkContext sc = new JavaSparkContext(conf);

        long startingTime = System.currentTimeMillis();

        JavaPairRDD<String, String> textsRDD = sc.wholeTextFiles("C:\\Users\\Sands\\IdeaProjects\\Master\\SparkBelegaufgabe\\texts\\*\\*.txt");

        JavaPairRDD<String, String> languageTextsRDD = textsRDD.mapToPair(tuple -> {
            String[] pathParts = tuple._1().split("/");
            String language = pathParts[pathParts.length - 2];
            String cleanedText = tuple._2().replaceAll("[^a-zA-Zа-яА-Я0-9\\s]", " ");
            return new Tuple2<>(language, cleanedText);
        });

        JavaPairRDD<String, String> concatenatedTextRDD = languageTextsRDD.reduceByKey((text1, text2) -> text1 + " " + text2);

        JavaPairRDD<String, String> longestWordsRDD = concatenatedTextRDD.mapValues(text -> {
            String[] words = text.split("\\s+");
            String longestWord = "";
            for (String word : words) {
                if (word.length() > longestWord.length()) {
                    longestWord = word;
                }
            }
            return longestWord;
        });

        /*longestWordsRDD.map(tuple -> {
            String language = tuple._1();
            String longestWord = tuple._2();
            int wordLength = longestWord.length();
            System.out.printf("%s - %s - %d%n", language, longestWord, wordLength);
            return String.format("%s - %s - %d", language, longestWord, wordLength);
        }).saveAsTextFile("C:\\Users\\Sands\\IdeaProjects\\Master\\SparkBelegaufgabe\\result");*/

        longestWordsRDD.foreach(tuple -> {
            String language = tuple._1();
            String longestWord = tuple._2();
            int wordLength = longestWord.length();
            System.out.printf("%s - %s - %d%n", language, longestWord, wordLength);
        });

        long endTime = System.currentTimeMillis();

        System.out.println("Run Time in MS:  " + (endTime - startingTime) + "ms");
        sc.stop();
    }
}
