import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class EditDistanceV1 {

    public static Set<Text> table1Words = null; // distinct words from table1.txt
    public static int wordLength;
    public static int gramLength = 2;
    public static int threshold;


    public static class Distance1Mapper extends Mapper<Object, Text, Text, Text> {

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String word = value.toString();

            System.out.printf("Index: %s, word: %s\n", key.toString(), word);

            for (Text s : table1Words) {
                context.write(s, new Text(word + "," +  commaSeparatedGrams(word)));
            }
        }
    }

    public static class Distance1Reducer extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] grams1 = commaSeparatedGrams(key.toString()).split(",");

            System.out.printf("Reduce key: %s", key.toString());

            for (Text t : values) {
                String value = t.toString();

                String[] grams2 = value.substring(wordLength + 1).split(",");

                if (enoughOverLappingGrams(grams1, grams2)) {
                    String word = value.substring(0, wordLength);
                    System.out.printf("Enough overlapping 2-grams for words %s and %s\n", key.toString(), word);
                    context.write(key, new Text(word));
                }
            }
        }
    }

    public static class Distance2Mapper extends Mapper<Text, Text, Text, Text> {

        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            String w1 = key.toString();
            String w2 = value.toString();

            if (editDistance(w1, w2) == threshold) {
                System.out.printf("Edit distance (%s, %s) == %d\n", w1, w2, threshold);
                context.write(key, value);
            }
        }
    }

    public static class Distance2Reducer extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            boolean first = true;

            for (Text value : values) {

                if (first) {
                    System.out.printf("Outputting (%s, %s)\n", key.toString(), value.toString());
                    context.write(key, value);
                    first = false;
                } else {
                    System.out.printf("Ignoring duplicate (%s, %s)\n", key.toString(), value.toString());
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

        Job job1 = Job.getInstance(conf1, "edit-distance job 1");
        job1.setJarByClass(EditDistanceV1.class);
        job1.setMapperClass(Distance1Mapper.class);
        job1.setReducerClass(Distance1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        Path middleOutPut = new Path("job1-out");

        FileInputFormat.addInputPath(job1, new Path(args[1]));

        FileOutputFormat.setOutputPath(job1, middleOutPut);


        // Initialise job2

        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "edit-distance job 2");
        job2.setJarByClass(EditDistanceV1.class);
        job2.setMapperClass(Distance2Mapper.class);
        job2.setReducerClass(Distance2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job2, middleOutPut);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));


        // Start execution

        System.out.printf("Reading table 1...\n");
        long startTime = System.currentTimeMillis();

        table1Words = readWords(table1FilePath);

        long endTime = System.currentTimeMillis();
        System.out.printf("Table 1 read. Found %d distinct words. Took %d milliseconds\"\n\n", table1Words.size(), endTime - startTime);

        if (job1.waitForCompletion(true)) {
            System.out.println("\nJob1 execution complete...\n");
            System.exit(job2.waitForCompletion(true) ? 0: 1);
        }
    }

    private static Set<Text> readWords(String tableFilePath) throws IOException {

        Set<Text> words = new TreeSet<Text>();

        BufferedReader bufferedReader = new BufferedReader(new FileReader(tableFilePath));
        String word = null;

        while ((word = bufferedReader.readLine()) != null) {
            if (! words.add(new Text(word))) {
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
