import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

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
import org.apache.hadoop.util.bloom.Key

public class ScoreStudentBloomFilter {

    public static class ScoresMapper extends Mapper <Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");

            int score1 = Integer.parseInt(parts[1]);
            int score2 = Integer.parseInt(parts[2]);

            if (score1 > 80 && score2 <= 95) {
                context.write(new Text(parts[0]), new Text("score\t" + mkString(parts, 1)));
            }
        }
    }

    /**
     * 1. Initialise a bloom filter in the setup() method of the Mapper class (the filter object itself should be global so that it can be accessed by the map() method later):

     2. filter = new BloomFilter(VECTOR_SIZE,NB_HASH,HASH_TYPE);

     3. Read the smaller table into the setup() method of the Mapper.

     4. Add the ID of each record to a bloom filter:
        filter.add(ID);

     5. In the map() method itself, use filter.membershipTest(ID) on any IDs from the larger input source.
        If there is no match, you know that the ID isn't present in your smaller dataset, and so shouldn't be passed to the reducer.

        Remember that you will get false positives in the reducer, so don't assume everything will be joined.
     */

    public static class StudentsMapper extends Mapper <Object, Text, Text, Text> {

        private BloomFilter scoreFilter = new BloomFilter(10000, 100, 0);


        protected void setup(Context context) throws IOException, InterruptedException {

            URI fileUri = context.getCacheFiles()[0];
            String filePath = fileUri.getPath();

            try {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
                String score = null;

                while ((score = bufferedReader.readLine()) != null) {

                    String[] parts = score.split(",");

                    int score1 = Integer.parseInt(parts[1]);
                    int score2 = Integer.parseInt(parts[2]);

                    if (score1 > 80 && score2 <= 95) {
                        scoreFilter.add(new Key(parts[0].getBytes()));
                    }
                }
            } catch (IOException ex) {
                System.err.printf("Exception while reading file %s. Exception: %s", filePath, ex.getMessage());
            }
        }


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

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

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
                String studentId = key.toString();

                System.out.printf("Writing data for student %s\n", studentId);
                context.write(NullWritable.get(), new Text(studentId + "," + studentData + scoresData));
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

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
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
