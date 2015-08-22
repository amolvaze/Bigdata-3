/********************************************************************
 * Program by:- Amol Vaze  & Net id:- asv130130@utdallas.edu
 Program lists all movie titles where genre is Fantasy and it uses movies.dat
 ********************************************************************/
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question3 {
	// Global variable declaration.
	public static String gen_name;

	public static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		// public void configure(JobConf job) {
		// genre_name = job.get("genre");
		// }

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String in_str = value.toString();

		// Splitting the given attributes
			String[] array = in_str.split("::");
		    String movie_title = array[1];
			String gen_lst = array[2];
			//Splitting the genre list into individual genres.
			String genre[] = gen_lst.split("\\|");
            System.out.println(gen_name);
			Configuration conf = context.getConfiguration();
			gen_name = conf.get("genre");
			for (int i = 0; i < genre.length; i++) {
				if (genre[i].equals(gen_name)) {
					context.write(new Text(movie_title), NullWritable.get());
				}
			}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}

		conf.set("genre", args[2]);

		/*Copy the genre name into global variable.
		JobConf jobConf = new JobConf(MovieGenreFilter.class);
		jobConf.set("genre", args[2]);
		 genre_name = args[2].toString();
		genre_name.replaceAll(" ", "");
		System.out.println(genre_name);*/

		// create a job with name "Question3"
		// @SuppressWarnings("deprecation")
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "movietitlelist");
		job.setJarByClass(Question3.class);
		job.setMapperClass(Map.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(NullWritable.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
