package com.couchbase.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

import com.couchbase.hadoop.CouchbaseConfig;
import com.couchbase.hadoop.mapred.CouchbaseInputFormat;

public class PrintEverythingMapred {

	public static class DumpMapper implements
			Mapper<BytesWritable, BytesWritable, Text, Text> {
		@Override
		public void configure(JobConf conf) {

		}

		@Override
		public void close() throws IOException {

		}

		@Override
		public void map(BytesWritable kbw, BytesWritable vbw,
				OutputCollector<Text, Text> arg2, Reporter reporter) {
			String key = new String(kbw.getBytes());
			String val = new String(vbw.getBytes());
			System.out.println("K,V: " + key + ", " + val);
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		final JobConf job = new JobConf("dumper");
		job.set(CouchbaseConfig.CB_INPUT_CLUSTER, "http://127.0.0.1:8091/");

		job.setJarByClass(PrintEverything.class);
		job.setMapperClass(DumpMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormat(CouchbaseInputFormat.class);
		job.setOutputFormat(NullOutputFormat.class);

		JobClient.runJob(job);
	}
}
