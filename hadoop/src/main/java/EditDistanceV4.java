import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class EditDistanceV4 {

    public static int wordLength, threshold;
    public static int gramLength = 2;

    public static class Distance1T1Mapper extends Mapper<Object, Text, Text, Text> {

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String word = value.toString();
            List<String> output = firstMapPhase(word);

            for (String gramComb : output) {
                context.write(new Text(gramComb), new Text("\t1" + word));
            }
        }
    }

    public static class Distance1T2Mapper extends Mapper<Object, Text, Text, Text> {

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String word = value.toString();
            List<String> output = firstMapPhase(word);

            for (String gramComb : output) {
                context.write(new Text(gramComb), new Text("\t2" + word));
            }
        }
    }

    public static class Distance1Reducer extends Reducer<Text, Text, NullWritable, Text> {

        protected void reduce(Text gramCombination, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> t1Words = new ArrayList<String>();
            List<String> t2Words = new ArrayList<String>();

            for (Text t : values) {

                String w = t.toString();

                if (w.startsWith("\t1")) {
                    t1Words.add(w.substring(2));
                } else {
                    t2Words.add(w.substring(2));
                }
            }

            for (String word1 : t1Words) {
                for (String word2 : t2Words) {

                    if (editDistance(word1, word2) <= threshold) {
                        context.write(NullWritable.get(), new Text(word1 + "," + word2));
                    }
                }
            }
        }
    }

    public static class Distance2Mapper extends Mapper<Object, Text, Text, Text> {

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] words = value.toString().split(",");

            if (words.length != 2) {
                throw new RuntimeException("Invalid length for words " + words.length + " in Distance2Mapper");
            }

            int first = 0;
            if (words[0].compareTo(words[1]) > 0) {
                first = 1;
            }
            int second = first == 0 ? 1 : 0;

            context.write(new Text(words[first]), new Text(words[second]));
        }
    }

    public static class Distance2Reducer extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> found = new ArrayList<String>();

            for (Text t : values) {

                String w = t.toString();

                if (! found.contains(w)) {
                    found.add(w);
                }
            }

            for (String s : found) {
                context.write(key, new Text(s));
            }
        }
    }


    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();

        if (args.length < 5) {
            throw new IllegalArgumentException("Too few arguments given.");
        }

        // Init job1

        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "edit-distance job 1");
        job1.setJarByClass(EditDistanceV4.class);
        job1.setReducerClass(Distance1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, Distance1T1Mapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, Distance1T2Mapper.class);

        Path middleOutPut = new Path("job1-out");

        FileInputFormat.addInputPath(job1, new Path(args[1]));

        FileOutputFormat.setOutputPath(job1, middleOutPut);


        // Init job2

        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "edit-distance job 2");
        job2.setJarByClass(EditDistanceV4.class);
        job2.setMapperClass(Distance2Mapper.class);
        job2.setReducerClass(Distance2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, middleOutPut);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));


        threshold = Integer.parseInt(args[3]);
        wordLength = Integer.parseInt(args[4]);


        // Start execution

        if (job1.waitForCompletion(true)) {
            System.out.printf("\n\nJob 1 execution complete... Took %d milliseconds.", System.currentTimeMillis() - startTime);

            if (job2.waitForCompletion(true)) {
                System.out.printf("\n\nJob 2 execution complete... Total running time %d milliseconds.", System.currentTimeMillis() - startTime);
            }
        }
    }


    public static List<String> firstMapPhase(String word) {

        List<String> output = new ArrayList<String>();

        List<String> grams = grams(word);
        List<ArrayList<String>> combinations = gramCombinations(grams, wordLength + 1 - gramLength * (threshold + 1));

        for (ArrayList<String> c : combinations) {
            output.add(gramCombinationToString(c));
        }

        return output;
    }

    private static List<String> grams(String s) {

        List<String> grams = new ArrayList<String>();

        for (int i = 0; i <= s.length() - gramLength; i ++) {
            grams.add(s.substring(i, i + gramLength));
        }

        return grams;
    }

    private static List<ArrayList<String>> gramCombinations(List<String> grams, int combinationsSize) {

        List<ArrayList<String>> subsets = new ArrayList<ArrayList<String>>();

        int[] s = new int[combinationsSize]; // indexes for grams to form combinations

        if (combinationsSize <= grams.size()) {

            for (int i = 0; (s[i] = i) < combinationsSize - 1; i++) ;
            subsets.add(getSubset(grams, s));

            for (; ; ) {
                int i;
                // find position of item that can be incremented
                for (i = combinationsSize - 1; i >= 0 && s[i] == grams.size() - combinationsSize + i; i--) ;
                if (i < 0) {
                    break;
                }
                s[i]++;                    // increment this item
                for (++i; i < combinationsSize; i++) {    // fill up remaining items
                    s[i] = s[i - 1] + 1;
                }
                subsets.add(getSubset(grams, s));
            }
        }

        return subsets;
    }


    private static ArrayList<String> getSubset(List<String> input, int[] subset) {
        ArrayList<String> result = new ArrayList<String>();

        for (int i = 0; i < subset.length; i++)
            result.add(input.get(subset[i]));
        return result;
    }

    private static String gramCombinationToString(List<String> combination) {

        Collections.sort(combination);

        StringBuilder s = new StringBuilder();

        for (int i = 0; i < combination.size() - 1; i++) {
            s.append(combination.get(i));
            s.append(",");
        }
        s.append(combination.get(combination.size() - 1));

        return s.toString();
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
