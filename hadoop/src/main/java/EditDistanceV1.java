import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EditDistanceV1 {

    public static Map<String, ArrayList<String>> overDistanceThreeFirstCharactersPairs = new HashMap<String, ArrayList<String>>();

    public static class DistanceMapper extends Mapper <Object, Text, Text, Text> {

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        }
    }


    public static class DistanceReducer extends Reducer <Text, Text, NullWritable, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "score-student bloom filter reduce join");
        job.setJarByClass(ScoreStudentBloomFilter.class);
        job.setReducerClass(DistanceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static int editDistance(String s1, String s2) {

        boolean firstS1 = true;

        String s1Sub = s1.substring(0, 2);
        String s2Sub = s2.substring(0, 2);

        int comparison = s1Sub.compareTo(s2Sub);

        if (comparison > 0) {
            firstS1 = false;
        }

        if (editDistanceKnownToBeLong(s1Sub, s2Sub, firstS1)) {
            return Integer.MAX_VALUE;
        }

        int distance = computeEditDistance(s1, s2);

        if (distance > 2) {

            if (firstS1) {
                ArrayList<String> currentPairs = overDistanceThreeFirstCharactersPairs.get(s1Sub);
                currentPairs.add(s2Sub);

                overDistanceThreeFirstCharactersPairs.put(s1, currentPairs);
            } else {
                ArrayList<String> currentPairs = overDistanceThreeFirstCharactersPairs.get(s2Sub);
                currentPairs.add(s1Sub);

                overDistanceThreeFirstCharactersPairs.put(s1, currentPairs);
            }

            distance = Integer.MAX_VALUE;
        }

        return distance;
    }

    private static boolean editDistanceKnownToBeLong(String s1Sub, String s2Sub, boolean firstS1) {

        if (firstS1) {
            ArrayList<String> distancesFromS1 = overDistanceThreeFirstCharactersPairs.get(s1Sub);

            if (distancesFromS1 != null) {
                return distancesFromS1.contains(s2Sub);
            }
        }

        else {
            ArrayList<String> distancesFromS2 = overDistanceThreeFirstCharactersPairs.get(s2Sub);

            if (distancesFromS2 != null) {
                return distancesFromS2.contains(s1Sub);
            }
        }

        return false;
    }

    private static int computeEditDistance(String s1, String s2) {

        int lengthS1 = s1.length();
        int lengthS2 = s2.length();

        int edits[][] = new int[lengthS1 + 1][lengthS2 + 1];

        for (int i = 0; i <= lengthS1; i++)
            edits[i][0] = i;

        for (int j = 1; j <= lengthS2; j++)
            edits[0][j] = j;


        for (int i = 1; i <= lengthS1; i++) {
            for (int j = 1; j <= lengthS2; j++) {

                edits[i][j]= Math.min(
                        edits[i-1][j] + 1,
                        Math.min(
                                edits[i][j-1] + 1,
                                edits[i-1][j-1] + s1.charAt(i-1) == s2.charAt(j-1) ? 0 : 1
                        )
                );
            }
        }
        return edits[lengthS1][lengthS2];
    }

}
