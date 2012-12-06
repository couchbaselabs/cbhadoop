package com.couchbase.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.vbucket.config.VBucket;
import com.couchbase.hadoop.CouchbaseConfig;

public class CouchbaseInputFormat implements InputFormat<BytesWritable, BytesWritable> {
	static class CouchbaseSplit implements InputSplit, Writable {
		final List<Integer> vbuckets;

		CouchbaseSplit() {
			vbuckets = new ArrayList<Integer>();
		}
		
		CouchbaseSplit(List<Integer> vblist) {
			vbuckets = vblist;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			short numvbuckets = in.readShort();
			for(int i = 0; i < numvbuckets; i++) {
				vbuckets.add(new Integer(in.readShort()));
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeShort(vbuckets.size());
			for(Integer v : vbuckets) {
				out.writeShort(v.shortValue());
			}
		}

		@Override
		public long getLength() throws IOException {
			return vbuckets.size();
		}

		@Override
		public String[] getLocations() {
			return new String[0];
		}
	}

	@Override
	public RecordReader<BytesWritable, BytesWritable> getRecordReader(InputSplit isplit,
			JobConf conf, Reporter reporter) throws IOException {
		final CouchbaseRecordReader reader = new CouchbaseRecordReader();
		reader.initialize(isplit, conf);
		return reader;
	}

	@Override
	public InputSplit[] getSplits(JobConf conf, int numsplits) throws IOException {
		final URI ClusterURI;
		try {
			ClusterURI = new URI(conf.get(CouchbaseConfig.CB_INPUT_CLUSTER));
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		final List<URI> ClientURIList = new ArrayList<URI>();
		ClientURIList.add(ClusterURI.resolve("/pools"));
		final String bucket = conf.get(CouchbaseConfig.CB_INPUT_BUCKET, "default");
		final String password = conf.get(CouchbaseConfig.CB_INPUT_PASSWORD, "");

		final CouchbaseConnectionFactory fact = new CouchbaseConnectionFactory(
				ClientURIList, bucket, password);

		final com.couchbase.client.vbucket.config.Config vbconfig = fact
				.getVBucketConfig();

		final List<VBucket> allVBuckets = vbconfig.getVbuckets();
		@SuppressWarnings("unchecked")
		final ArrayList<Integer>[] vblists = 
				new ArrayList[vbconfig.getServersCount()];
		int vbid = 0;
		for(VBucket v : allVBuckets) {
			if(vblists[v.getMaster()] == null) {
				vblists[v.getMaster()] = new ArrayList<Integer>();
			}
			vblists[v.getMaster()].add(vbid);
			vbid++;
		}
		final ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
		for(ArrayList<Integer> vblist : vblists) {
			splits.add(new CouchbaseSplit(vblist));
		}
		
		InputSplit[] returned = new InputSplit[splits.size()];
		splits.toArray(returned);
		return returned;
	}

}
