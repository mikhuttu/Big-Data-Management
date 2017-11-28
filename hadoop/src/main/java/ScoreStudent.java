import java.io.IOException;

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

public class ScoreStudent {

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

    public static class StudentsMapper extends Mapper <Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");

            int yearOfBirth = Integer.parseInt(parts[2]);

            if (yearOfBirth > 1990) {
                context.write(new Text(parts[0]), new Text("student\t" + mkString(parts, 1)));
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

        Job job = Job.getInstance(conf, "score-student reduce join");
        job.setJarByClass(ScoreStudent.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ScoresMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, StudentsMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);

        // outputPath.getFileSystem(conf).delete(outputPath);

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
