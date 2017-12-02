import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

public class EditDistanceV2 {

    public static Set<String> table1Words = null; // distinct comma separated words from table1.txt
    public static int wordLength;
    public static int gramLength = 2;
    public static int threshold;


    public static class DistanceMapper extends Mapper<Object, Text, Text, Text> {

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String word1 = value.toString();
            String[] grams1 = commaSeparatedGrams(word1).split(",");

            //System.out.printf("Index: %s, word: %s\n", key.toString(), word1);

            for (String word2WithGrams : table1Words) {
                String[] grams2 = word2WithGrams.substring(wordLength + 1).split(",");

                if (enoughOverLappingGrams(grams1, grams2)) {
                    String word2 = word2WithGrams.substring(0, wordLength);
                    System.out.printf("Enough overlapping 2-grams for words %s and %s\n", word1, word2);

                    if (editDistance(word1, word2) <= threshold) {
                        System.out.printf("Edit distance (%s, %s) <= %d\n", word1, word2, threshold);
                        context.write(new Text(word2), new Text(word1));
                    }
                }
            }
        }
    }

    public static class DistanceReducer extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Set<String> seen = new TreeSet<String>();
            String s = key.toString();

            for (Text value : values) {

                String next = value.toString();

                if (seen.contains(next)) {
                    System.out.printf("Ignoring duplicate (%s, %s)\n", s, next);
                } else {
                    System.out.printf("Outputting (%s, %s)\n", s, next);
                    context.write(key, value);

                    seen.add(next);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 5) {
            throw new IllegalArgumentException("Too few arguments given.");
        }

        String table1FilePath = args[0];
        if (args.length > 5) {
            table1FilePath = args[5] + table1FilePath;
        }

        threshold = Integer.parseInt(args[3]);
        wordLength = Integer.parseInt(args[4]);


        // Initialise job1

        Configuration conf1 = new Configuration();

        Job job = Job.getInstance(conf1, "edit-distance job 1");
        job.setJarByClass(EditDistanceV2.class);
        job.setMapperClass(DistanceMapper.class);
        job.setReducerClass(DistanceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));


        // Start execution

        System.out.printf("Reading table 1...\n");
        long startTime = System.currentTimeMillis();

        table1Words = readWords(table1FilePath);

        long endTime = System.currentTimeMillis();
        System.out.printf("Table 1 read. Found %d distinct words. Took %d milliseconds\"\n\n", table1Words.size(), endTime - startTime);

        // for (String w : table1Words) System.out.printf(w + "\n");

        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

    private static Set<String> readWords(String tableFilePath) throws IOException {

        Set<String> words = new TreeSet<String>();

        BufferedReader bufferedReader = new BufferedReader(new FileReader(tableFilePath));
        String word = null;

        while ((word = bufferedReader.readLine()) != null) {
            String w = word + "," + commaSeparatedGrams(word);

            if (! words.add(w)) {
                System.out.printf("Duplicate word %s in table1", word);
            }
        }

        return words;
    }

    public static String commaSeparatedGrams(String s) {

        StringBuilder gramsBuilder = new StringBuilder();

        for (int i = 0; i <= s.length() - gramLength; i ++) {

            if (i != 0) {
                gramsBuilder.append(",");
            }

            gramsBuilder.append(s.substring(i, i + gramLength));
        }

        return gramsBuilder.toString();
    }

    public static boolean enoughOverLappingGrams(String[] g1, String[] g2) {

        int overlapping = 0;
        int enough = wordLength + 1 - gramLength * (threshold + 1);

        for (String w1 : g1) {
            nextWord:

            for (String w2 : g2) {

                if (w1.equals(w2)) {
                    overlapping += 1;

                    if (overlapping >= enough) {
                        return true;
                    }

                    break nextWord;
                }
            }
        }

        return false;
    }

    public static int editDistance(String word1, String word2) {
        int len1 = word1.length();
        int len2 = word2.length();

        // len1+1, len2+1, because finally return dp[len1][len2]
        int[][] dp = new int[len1 + 1][len2 + 1];

        for (int i = 0; i <= len1; i++) {
            dp[i][0] = i;
        }

        for (int j = 0; j <= len2; j++) {
            dp[0][j] = j;
        }

        //iterate though, and check last char
        for (int i = 0; i < len1; i++) {
            char c1 = word1.charAt(i);
            for (int j = 0; j < len2; j++) {
                char c2 = word2.charAt(j);

                //if last two chars equal
                if (c1 == c2) {
                    //update dp value for +1 length
                    dp[i + 1][j + 1] = dp[i][j];
                } else {
                    int replace = dp[i][j] + 1;
                    int insert = dp[i][j + 1] + 1;
                    int delete = dp[i + 1][j] + 1;

                    int min = replace > insert ? insert : replace;
                    min = delete > min ? min : delete;
                    dp[i + 1][j + 1] = min;
                }
            }
        }

        return dp[len1][len2];
    }

}
