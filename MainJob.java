import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class MainJob {

  public static Optional<String> specificTypeExtract(JsonNode specificData) {
    return Optional.ofNullable(specificData.get("crawler_target"))
        .map(crawler_target -> crawler_target.get("specific_resource_type"))
        .map(JsonNode::asText);
  }

  public static Optional<String> socialMediaTypeExtract(JsonNode specificData) {
    return Optional.ofNullable(specificData.get("object"))
        .map(crawler_target -> crawler_target.get("social_media"))
        .map(JsonNode::asText);
  }

  public static String getType(JsonNode specData) {
    return socialMediaTypeExtract(specData)
        .orElse(specificTypeExtract(specData).orElse(null));
  }

  public static String getDateString(JsonNode json) {
    List<String> dateFields = Arrays.asList("created_time", "created_at", "snippet.publishedAt",
        "snippet.topLevelComment.snippet.publishedAt");
    for (String dateField : dateFields) {
      if (json.has(dateField)) {
        return json.get(dateField).asText();
      }
    }
    return null;
  }

  public static String constructDate(String s) {
    if (StringUtils.isNumeric(s)) {
      long epochVal = Long.parseLong(s);
      Date d = new Date(epochVal * 1000);
      SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
      return formatter.format(d);
    } else {
      // String of socmed date format, beside Instagram POSIX time format
      DateFormat[] possibleFormats = {
          new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy"),
          new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"),
          new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ"),
          new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
      };

      for (DateFormat format : possibleFormats) {
        try {
          Date result = format.parse(s);
          SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
          return formatter.format(result);
        } catch (ParseException ignored) {

        }
      }
    }
    return null;
  }

  public static void main(String[] args) {
    String inputPath = args[0];
    String outputPath = args[1];

    // Create a Spark configuration
    SparkConf conf = new SparkConf()
        .setAppName("MainJob")
        .setMaster("local[*]");

    // Create Spark context
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Read all the JSON files
    JavaRDD<String> input = sc.textFile(inputPath);

    // Extract relevant fields and convert to tuples
    JavaPairRDD<Tuple2<String, String>, Integer> tuples = input
        .flatMapToPair(t -> {
          List<Tuple2<Tuple2<String, String>, Integer>> a = new ArrayList<>();
          try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(t);
            for (JsonNode j : rootNode) {
              String date = constructDate(getDateString(j));
              if (date != null) {
                String soc_med = getType(j);
                a.add(new Tuple2<>(new Tuple2<>(date, soc_med), 1));
              }
            }
          } catch (Exception e) {
            System.out.println("Error reading tree, err val : " + e);
          }
          return a.iterator();
        });

    JavaPairRDD<Tuple2<String, String>, Integer> result = tuples.reduceByKey(Integer::sum);

    // SORT
    JavaPairRDD<Tuple2<String, String>, Integer> sortedResult = result
        .sortByKey(Comparator.comparing((Tuple2<String, String> t) -> t._1())
            .thenComparing(Tuple2::_2));

    // Write the sorted results to a text file
    JavaRDD<String> mappedResult = sortedResult.map(
        pair -> pair._1()._1() + "," + pair._1()._2() + "," + pair._2().toString());
    mappedResult.coalesce(1).saveAsTextFile(outputPath);

    sc.close();
  }

}
