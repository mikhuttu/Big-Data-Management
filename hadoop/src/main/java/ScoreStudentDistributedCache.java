import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
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

public class ScoreStudentDistributedCache {

    public static class JoinMapper extends Mapper <Object, Text, Text, Text> {

        private Map<String, String> scores = new HashMap<String, String>();


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
                        scores.put(parts[0], mkString(parts, 1));
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
                String scoresString = scores.get(parts[0]);

                if (scoresString != null) {
                    context.write(new Text(parts[0]), new Text(mkString(parts, 1) + scoresString));
                }
            }
        }
    }

    public static class ScoreStudentDistributedCacheReducer extends Reducer <Text, Text, NullWritable, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text data : values) {
                String studentId = key.toString();

                System.out.printf("Writing data for student %s\n", studentId);
                context.write(NullWritable.get(), new Text(studentId + "," + data));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "score-student distributed cache join");
        job.setJarByClass(ScoreStudentDistributedCache.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(ScoreStudentDistributedCacheReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(args[0]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[1]));
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
