import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
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
    SparkConf conf = new SparkConf().setAppName("MainJob").setMaster("local[*]");

    // Create Spark context
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Read all the JSON files
    JavaRDD<String> input = sc.textFile(inputPath);

    // Extract relevant fields and convert to tuples
    JavaPairRDD<Tuple2<String, String>, Integer> tuples = input
        .flatMapToPair((PairFlatMapFunction<String, Tuple2<String, String>, Integer>) t -> {
          ObjectMapper mapper = new ObjectMapper();
          List<Tuple2<Tuple2<String, String>, Integer>> a = new ArrayList<>();

          try {
            JsonNode rootNode = mapper.readTree(t);
            for (JsonNode j : rootNode) {
              // Extract the date, social media, and value fields
              String date = constructDate(getDateString(j));
              if (date == null) {
                continue;
              }
              String soc_med = getType(j);
              if (soc_med == null) {
                continue;
              }
              Integer count = 1;
              a.add(new Tuple2<>(new Tuple2<>(date, soc_med), count));
            }
            return a.iterator();

          } catch (Exception e) {
            System.out.println("Error reading tree, err val : " + e);
          }
          return a.iterator();
        });

    JavaPairRDD<Tuple2<String, String>, Integer> result = tuples
        .reduceByKey(new Function2<Integer, Integer, Integer>() {
          private static final long serialVersionUID = 1L;

          @Override
          public Integer call(Integer v1, Integer v2) {
            return v1 + v2;
          }
        });

    class CustomComparator implements Comparator<Tuple2<String, String>>, Serializable {

      @Override
      public int compare(Tuple2<String, String> t1, Tuple2<String, String> t2) {
        try {
          SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd");
          Date a = parser.parse(t1._1());
          Date b = parser.parse(t2._1());

          int res = a.compareTo(b);

          if (res == 0) {
            return t1._2().compareTo(t2._2());
          }

          return res;
        } catch (Exception e) {
          System.out.println("Failed at parsing dates, doing the string comparison");
        }
        return t1._1().compareTo(t2._1());
      }
    }
    // SORT
    JavaPairRDD<Tuple2<String, String>, Integer> sortedResult = result.sortByKey(
        new CustomComparator());

    // Write the sorted results to a text file
    JavaRDD<String> mappedResult = sortedResult.map(
        pair -> pair._1()._1() + "," + pair._1()._2() + "," + pair._2().toString());
    mappedResult.coalesce(1).saveAsTextFile(outputPath);

    sc.close();
  }
}
