package com.couchbase.hadoop.mapred;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.naming.ConfigurationException;

import net.spy.memcached.tapmessage.RequestMessage;
import net.spy.memcached.tapmessage.ResponseMessage;
import net.spy.memcached.tapmessage.TapMagic;
import net.spy.memcached.tapmessage.TapOpcode;
import net.spy.memcached.tapmessage.TapRequestFlag;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import com.couchbase.client.TapClient;
import com.couchbase.hadoop.CouchbaseConfig;
import com.couchbase.hadoop.mapred.CouchbaseInputFormat.CouchbaseSplit;

public class CouchbaseRecordReader implements RecordReader<BytesWritable, BytesWritable> {
	TapClient tapclient;
	ResponseMessage lastmessage;
	final UUID uuid = UUID.randomUUID();

	@Override
	public void close() throws IOException {
		tapclient.shutdown();
	}
	
	public String getCurrentKey() throws IOException {
		if (lastmessage == null)
			return null;
		return lastmessage.getKey();
	}

	public byte[] getCurrentValue() throws IOException {
		if (lastmessage == null)
			return null;
		return lastmessage.getValue();
	}

	@Override
	public float getProgress() {
		if (!tapclient.hasMoreMessages()) {
			return 1;
		}
		return 0;
	}

	public void initialize(InputSplit isplit, JobConf conf)
			throws IOException {
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
				+ uuid.toString();
		tapReq.setName(streamName);

		tapclient = new TapClient(ClientURIList, bucket, password);
		try {
			tapclient.tapCustom(streamName, tapReq);
		} catch (ConfigurationException e) {
			throw new IOException(e);
		}
	}

	public boolean nextKeyValue() {
		while (tapclient.hasMoreMessages()) {
			lastmessage = tapclient.getNextMessage(10, TimeUnit.SECONDS);
			if (lastmessage != null) {
				return true;
			}
		}
		return false;
	}

	@Override
	public BytesWritable createKey() {
		return new BytesWritable();
	}

	@Override
	public BytesWritable createValue() {
		return new BytesWritable();
	}

	@Override
	public long getPos() throws IOException {
		return 0;
	}

	@Override
	public boolean next(BytesWritable k, BytesWritable v)
			throws IOException {
		if(nextKeyValue()) {
			k.set(new BytesWritable(getCurrentKey().getBytes()));
			v.set(new BytesWritable(getCurrentValue()));
			return true;
		}
		return false;
	}

}
