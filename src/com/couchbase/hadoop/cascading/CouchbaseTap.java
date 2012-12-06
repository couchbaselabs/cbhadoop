package com.couchbase.hadoop.cascading;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.SourceTap;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;

import com.couchbase.hadoop.CouchbaseConfig;
import com.couchbase.hadoop.mapred.CouchbaseInputFormat;

public class CouchbaseTap extends
		SourceTap<JobConf, RecordReader<BytesWritable, BytesWritable>> {

	private static final long serialVersionUID = 1L;

	public static class CouchbaseSourceScheme
			extends
			Scheme<JobConf, RecordReader<BytesWritable, BytesWritable>, Void, Object[], Void> {

		private static final long serialVersionUID = 1L;
		final String uri;
		final String bucket;
		final String password;
		final String id;

		public CouchbaseSourceScheme(String uri, String bucket,
				String password, Fields fs, String id) {
			super(fs);
			this.uri = uri;
			this.bucket = bucket;
			this.password = password;
			this.id = id;
		}

		@Override
		public void sink(FlowProcess<JobConf> flowProcess,
				SinkCall<Void, Void> sinkCall) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public void sinkConfInit(
				FlowProcess<JobConf> fp_,
				Tap<JobConf, RecordReader<BytesWritable, BytesWritable>, Void> tap_,
				JobConf conf_) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public boolean source(
				FlowProcess<JobConf> flowProcess,
				SourceCall<Object[], RecordReader<BytesWritable, BytesWritable>> sourceCall)
				throws IOException {
			BytesWritable key = (BytesWritable) sourceCall.getContext()[0];
			BytesWritable value = (BytesWritable) sourceCall.getContext()[1];

			boolean result = sourceCall.getInput().next(key, value);

			if (!result)
				return false;
			
			Tuple output = new Tuple(
					new String(key.getBytes(), 0, key.getLength(), "UTF-8"),
					new String(value.getBytes(), 0, value.getLength(), "UTF-8"));
			sourceCall.getIncomingEntry().setTuple(output);
			sourcePrepare(flowProcess, sourceCall);
			return true;
		}

		@Override
		public void sourcePrepare(
				FlowProcess<JobConf> flowProcess,
				SourceCall<Object[], RecordReader<BytesWritable, BytesWritable>> sourceCall) {
			sourceCall.setContext(new Object[2]);
			sourceCall.getContext()[0] = sourceCall.getInput().createKey();
			sourceCall.getContext()[1] = sourceCall.getInput().createValue();
		}

		@Override
		public void sourceConfInit(
				FlowProcess<JobConf> flowProcess,
				Tap<JobConf, RecordReader<BytesWritable, BytesWritable>, Void> tap,
				JobConf job) {
			FileInputFormat.setInputPaths(job, this.id);
			job.setInputFormat(CouchbaseInputFormat.class);
			job.set(CouchbaseConfig.CB_INPUT_BUCKET, bucket);
			job.set(CouchbaseConfig.CB_INPUT_CLUSTER, uri);
			job.set(CouchbaseConfig.CB_INPUT_PASSWORD, password);
		}

		public String getId() {
			return id;
		}
	}

	final String id;

	public CouchbaseTap(String uri, String bucket, String password, Fields fs) {
		super(new CouchbaseSourceScheme(uri, bucket, password, fs, 
				"/" + UUID.randomUUID().toString() + "-couchbase"));
		this.id = ((CouchbaseSourceScheme) this.getScheme()).getId();
	}

	@Override
	public String getIdentifier() {
		return getPath().toString();
	}

	public Path getPath() {
		return new Path(id);
	}

	@Override
	public boolean equals(Object object) {
		if (!getClass().equals(object.getClass())) {
			return false;
		}
		CouchbaseTap other = (CouchbaseTap) object;
		return id.equals(other.id);
	}

	@Override
	public long getModifiedTime(JobConf arg0) throws IOException {
		return System.currentTimeMillis();
	}

	@Override
	public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess,
			RecordReader<BytesWritable, BytesWritable> reader)
			throws IOException {
		return new HadoopTupleEntrySchemeIterator(flowProcess, this, reader);
	}

	@Override
	public boolean resourceExists(JobConf arg0) throws IOException {
		return true;
	}

}
