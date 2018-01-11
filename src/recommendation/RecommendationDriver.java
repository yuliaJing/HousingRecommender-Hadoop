// Author - Haoan Yan, Yuting Jing
package recommendation;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;


import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

public class RecommendationDriver {

	public static void main(String[] args) throws IOException {
		String input = args[0];
		String output = args[1];
		runViewerJob(input, output);
	}
	
	public static void runViewerJob(String input, String output) throws IOException {
		JobClient client = new JobClient();
		JobConf jobConf = new JobConf(RecommendationDriver.class);
		jobConf.setJobName("Airbnb Recommendation");


		jobConf.setMapperClass(RecommendationMapper.class);
		jobConf.setReducerClass(RecommendationReducer.class);
		jobConf.setCombinerClass(RecommendationReducer.class);


		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);


		Path outPath = new Path(output);
		FileInputFormat.addInputPath(jobConf, new Path(input));
		FileOutputFormat.setOutputPath(jobConf, outPath);

		FileSystem dfs = FileSystem.get(outPath.toUri(), jobConf);
		if (dfs.exists(outPath)) {
			dfs.delete(outPath, true);
		}

		client.setConf(jobConf);
		try {
			JobClient.runJob(jobConf);
		} catch (IOException ex) {
			Logger.getLogger(RecommendationDriver.class.getName()).log(
					Level.SEVERE, null, ex);
		}
		
		
	}
}
