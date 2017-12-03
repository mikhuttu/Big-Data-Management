import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class EditDistanceV3 {

    public static String alphabet = "01234567890abcdefgm";
    public static int wordLength, threshold;


    public static class DistanceT1Mapper extends Mapper<Object, Text, Text, Text> {

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, new Text("\t1" + value.toString()));
        }
    }

    public static class DistanceT2Mapper extends Mapper<Object, Text, Text, Text> {

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String originalWord = value.toString();

            Set<String> nearestNeighbours = new HashSet<String>();
            nearestNeighbours.add(originalWord);

            for (int t = threshold; t > 0; t--) {
                Set<String> current = new HashSet<String>(nearestNeighbours);

                for (String word : current) {
                    Set<String> nearest = nextInEditDistance(word, t - 1);
                    nearestNeighbours.addAll(nearest);
                }
            }

            for (String neighbour : nearestNeighbours) {
                if (neighbour.length() == wordLength) {
                    context.write(new Text(neighbour), new Text("\t2" + originalWord));
                }
            }
        }
    }

    public static class DistanceReducer extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Set<String> nearestNeighborsInTable2 = new HashSet<String>();
            boolean foundInTable1 = false;

            for (Text v : values) {

                if (v.toString().startsWith("\t1")) {
                    foundInTable1 = true;
                } else {
                    nearestNeighborsInTable2.add(v.toString().substring(2));
                }
            }

            if (foundInTable1) {
                for (String neighbour : nearestNeighborsInTable2) {
                    context.write(key, new Text(neighbour));
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();

        if (args.length < 5) {
            throw new IllegalArgumentException("Too few arguments given.");
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "edit-distance job 1");
        job.setJarByClass(EditDistanceV3.class);
        job.setReducerClass(DistanceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, DistanceT1Mapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DistanceT2Mapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        threshold = Integer.parseInt(args[3]);
        wordLength = Integer.parseInt(args[4]);


        /*String originalWord = "0123456789ab";

        Set<String> nearestNeighbours = new HashSet<String>();
        nearestNeighbours.add(originalWord);

        for (int t = threshold; t > 0; t--) {
            Set<String> current = new HashSet<String>(nearestNeighbours);

            for (String word : current) {
                Set<String> nearest = nextInEditDistance(word, t - 1);
                nearestNeighbours.addAll(nearest);
            }
        }

        int i = 0;
        for (String neighbour : nearestNeighbours) {
            if (neighbour.length() == wordLength) {
                i += 1;
                System.out.println(neighbour);
            }

        }
        System.out.println(i);*/

        // Start execution

        if (job.waitForCompletion(true)) {
            System.out.printf("\n\nExecution complete... Took %d milliseconds.", System.currentTimeMillis() - startTime);
        }
    }

    /**
     * Finds words that are in distance 1 of given word.
     * Ignores words if there are not enough further rounds to find new words of length == wordLength.
     * @param word
     * @param roundsLeft
     * @return
     */
    public static Set<String> nextInEditDistance(String word, int roundsLeft) {

        Set<String> nearestWords = new HashSet<String>();

        // all deletions

        if (word.length() - 1 + roundsLeft >= wordLength) {
            for (int i = 0; i < word.length(); i++) {
                nearestWords.add(word.substring(0, i) + word.substring(i + 1, word.length()));
            }
        }

        // all additions

        if (word.length() + 1 - roundsLeft <= wordLength) {
            for (int i = 0; i < word.length(); i++) {
                for (int j = 0; j < alphabet.length(); j++) {
                    nearestWords.add(word.substring(0, i) + alphabet.charAt(j) + word.substring(i, word.length()));
                }
            }

            for (int j = 0; j < alphabet.length(); j++) nearestWords.add(word + alphabet.charAt(j));
        }

        // all substitutions

        if (word.length() + roundsLeft >= wordLength || word.length() - roundsLeft <= wordLength) {
            for (int j = 0; j < alphabet.length(); j++) nearestWords.add(alphabet.charAt(j) + word.substring(1));

            for (int i = 1; i < word.length(); i++) {
                for (int j = 0; j < alphabet.length(); j++) {
                    char c = alphabet.charAt(j);

                    if (word.charAt(i) != c) {
                        nearestWords.add(word.substring(0, i) + c + word.substring(i + 1, word.length()));
                    }
                }
            }
        }

        return nearestWords;
    }

}
