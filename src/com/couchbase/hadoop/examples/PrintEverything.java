package com.couchbase.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.couchbase.hadoop.CouchbaseConfig;
import com.couchbase.hadoop.CouchbaseInputFormat;

public class PrintEverything {
	public static class DumpMapper extends Mapper<String, byte[], Text, Text> {
		public void map(String key, byte[] value, Context ctx) {
			String vstr = "null";
			if(value != null) {
				vstr = new String(value);
			}
			System.out.println("K,V: " + key + ", " + vstr);
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		final Configuration conf = new Configuration();
		conf.set(CouchbaseConfig.CB_INPUT_CLUSTER,
				"http://127.0.0.1:8091/");
		
		final Job job = new Job(conf, "dumper");
		
		job.setJarByClass(PrintEverything.class);
		job.setMapperClass(DumpMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(CouchbaseInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
