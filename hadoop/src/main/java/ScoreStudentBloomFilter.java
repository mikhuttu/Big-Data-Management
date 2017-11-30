import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class ScoreStudentBloomFilter {

    public static BloomFilter scoreFilter = null;

    public static class ScoresMapper extends Mapper <Object, Text, Text, Text> {

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            int score1 = Integer.parseInt(parts[1]);
            int score2 = Integer.parseInt(parts[2]);

            if (score1 > 80 && score2 <= 95) {
                context.write(new Text(parts[0]), new Text("score\t" + mkString(parts, 1)));
            }
        }
    }


    public static class StudentsMapper extends Mapper <Object, Text, Text, Text> {

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            int yearOfBirth = Integer.parseInt(parts[2]);

            if (yearOfBirth > 1990) {

                if (scoreFilter.membershipTest(new Key(parts[0].getBytes()))) {
                    context.write(new Text(parts[0]), new Text("student\t" + mkString(parts, 1)));
                }
            }
        }
    }


    public static class JoinReducer extends Reducer <Text, Text, NullWritable, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String scoresData = null;
            String studentData = null;

            for (Text t : values) {
                if (scoresData != null && studentData != null) {
                    System.out.printf("Scores and student data filled. Got still text %s\n", t.toString());
                }

                String parts[] = t.toString().split("\t");

                if (parts[0].equals("score")) {
                    scoresData = parts[1];
                }
                else if (parts[0].equals("student")) {
                    studentData = parts[1];
                }
            }

            if (scoresData != null && studentData != null) {
                context.write(NullWritable.get(), new Text(key.toString() + "," + studentData + "," +  scoresData));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "score-student bloom filter reduce join");
        job.setJarByClass(ScoreStudentBloomFilter.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ScoresMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, StudentsMapper.class);

        String scoreFileReadPath = args[0];
        if (args.length > 4) {
            scoreFileReadPath = args[4] + scoreFileReadPath;
        }

        System.out.printf("Initialising bloom filter...\n");
        long startTime = System.currentTimeMillis();

        scoreFilter = initBloomFilter(scoreFileReadPath, Integer.parseInt(args[3]));

        long endTime = System.currentTimeMillis();
        System.out.printf("Bloom filter initialised. Took %d milliseconds\"\n\n", endTime - startTime);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static BloomFilter initBloomFilter(String scoreFilePath, int scoreSize) throws IOException {

        int vectorSize, nbHash;
        int hashType = 0;

        // Did some research to get even relatively good values for vectorsize and the numbers of hash functions used
        // Ended up using https://hur.st/bloomfilter
        // Estimated that n is around 10% of the gone though scores. Picked p as 1.0E-7.

        if (scoreSize < 10000) {
            vectorSize = 34000 / 8;
            nbHash = 23;
        } else if (scoreSize < 10000000) {
            vectorSize = 34000000 / 8;
            nbHash = 23;
        } else {
            vectorSize = 340000000 / 8;
            nbHash = 23;
        }

        BloomFilter filter = new BloomFilter(vectorSize, nbHash, hashType);

        BufferedReader bufferedReader = new BufferedReader(new FileReader(scoreFilePath));
        String score = null;

        while ((score = bufferedReader.readLine()) != null) {

            String[] parts = score.split(",");

            int score1 = Integer.parseInt(parts[1]);
            int score2 = Integer.parseInt(parts[2]);

            if (score1 > 80 && score2 <= 95) {
                filter.add(new Key(parts[0].getBytes()));
            }
        }

        return filter;
    }

    public static String mkString(String[] parts, int first) {
        StringBuilder result = new StringBuilder();

        for (int i = first; i < parts.length - 1; i++) {
            result.append(parts[i] + ",");
        }

        result.append(parts[parts.length - 1]);
        return result.toString();
    }

}
