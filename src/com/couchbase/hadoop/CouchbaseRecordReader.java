package com.couchbase.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.naming.ConfigurationException;

import net.spy.memcached.tapmessage.RequestMessage;
import net.spy.memcached.tapmessage.ResponseMessage;
import net.spy.memcached.tapmessage.TapMagic;
import net.spy.memcached.tapmessage.TapOpcode;
import net.spy.memcached.tapmessage.TapRequestFlag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.couchbase.client.TapClient;
import com.couchbase.hadoop.CouchbaseInputFormat.CouchbaseSplit;

public class CouchbaseRecordReader extends RecordReader<String, byte[]> {

	TapClient tapclient;
	ResponseMessage lastmessage;

	@Override
	public void close() throws IOException {
		tapclient.shutdown();
	}

	@Override
	public String getCurrentKey() throws IOException, InterruptedException {
		if (lastmessage == null)
			return null;
		return lastmessage.getKey();
	}

	@Override
	public byte[] getCurrentValue() throws IOException, InterruptedException {
		if (lastmessage == null)
			return null;
		return lastmessage.getValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (!tapclient.hasMoreMessages()) {
			return 1;
		}
		return 0;
	}

	@Override
	public void initialize(InputSplit isplit, TaskAttemptContext ctx)
			throws IOException {
		final Configuration conf = ctx.getConfiguration();
		final URI ClusterURI;
		try {
			ClusterURI = new URI(conf.get(CouchbaseConfig.CB_INPUT_CLUSTER));
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		final List<URI> ClientURIList = new ArrayList<URI>();
		ClientURIList.add(ClusterURI.resolve("/pools"));
		final String bucket = conf.get(CouchbaseConfig.CB_INPUT_BUCKET,
				"default");
		final String password = conf.get(CouchbaseConfig.CB_INPUT_PASSWORD, "");

		RequestMessage tapReq = new RequestMessage();
		tapReq.setMagic(TapMagic.PROTOCOL_BINARY_REQ);
		tapReq.setOpcode(TapOpcode.REQUEST);
		tapReq.setFlags(TapRequestFlag.DUMP);
		tapReq.setFlags(TapRequestFlag.SUPPORT_ACK);
		tapReq.setFlags(TapRequestFlag.FIX_BYTEORDER);
		tapReq.setFlags(TapRequestFlag.LIST_VBUCKETS);
		final CouchbaseSplit split = (CouchbaseSplit) isplit;
		short[] vbids = new short[split.vbuckets.size()];
		int i = 0;
		for (Integer vbnum : split.vbuckets) {
			vbids[i] = vbnum.shortValue();
			i++;
		}
		final String namebase = conf.get(CouchbaseConfig.CB_INPUT_STREAM_NAME,
				"hadoop");
		tapReq.setVbucketlist(vbids);
		final String streamName = namebase + "_"
				+ ctx.getTaskAttemptID().toString();
		tapReq.setName(streamName);

		tapclient = new TapClient(ClientURIList, bucket, password);
		try {
			tapclient.tapCustom(streamName, tapReq);
		} catch (ConfigurationException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		while (tapclient.hasMoreMessages()) {
			lastmessage = tapclient.getNextMessage(10, TimeUnit.SECONDS);
			if (lastmessage != null) {
				return true;
			}
		}
		return false;
	}

}
