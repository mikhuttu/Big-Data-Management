import java.io.IOException;

import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ScoreStudent {

    public static class ScoresMapper extends Mapper <Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");

            int score1 = Integer.parseInt(parts[1]);
            int score2 = Integer.parseInt(parts[2]);

            if (score1 > 80 && score2 <= 95) {
                context.write(new Text(parts[0]), new Text("score\t" + mkString(parts, 1)));
            } else {
                System.out.printf("Score1: %d, score2: %d", score1, score2);
            }
        }
    }

    public static class StudentsMapper extends Mapper <Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");

            int yearOfBirth = Integer.parseInt(parts[2]);

            if (yearOfBirth > 1990) {
                context.write(new Text(parts[0]), new Text("student\t" + mkString(parts, 1)));
            } else {
                System.out.printf("Year of birth: %d", yearOfBirth);
            }
        }
    }

    public static class JoinReducer extends Reducer <Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String scoresData = "";
            String studentData = "";

            if (Iterables.size(values) == 2) {
                for (Text t : values) {
                    String parts[] = t.toString().split("\t");

                    if (parts[0].equals("score")) {
                        scoresData = mkString(parts, 1);
                    }
                    else if (parts[0].equals("student")) {
                        studentData = mkString(parts, 1);
                    }
                }

                String result = studentData + scoresData;

                context.write(key, new Text(studentData + scoresData));
            } else {
                System.out.printf("Size of iterables for key %s is %d", key.toString(), Iterables.size(values));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "score-student reduce join");
        job.setJarByClass(ScoreStudent.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, ScoresMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, StudentsMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);

        // outputPath.getFileSystem(conf).delete(outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static String mkString(String[] parts, int first) {
        StringBuilder result = new StringBuilder();

        for (int i = first; i < parts.length - 1; i++) { // ignore the first entry
            result.append(parts[i] + ",");
        }

        result.append(parts[parts.length - 1]);
        return result.toString();
    }

}
