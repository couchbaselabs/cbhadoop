package com.couchbase.hadoop.cascading.example;

import java.util.Properties;

import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.couchbase.hadoop.cascading.CouchbaseTap;
import com.twitter.maple.tap.StdoutTap;

public class PrintEverythingCascading {

	public static void main(String[] args) {
		Properties properties = new Properties();
	    AppProps.setApplicationJarClass(properties, PrintEverythingCascading.class);
	    HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);
		Tap cb = new CouchbaseTap("http://127.0.0.1:8091", "default", "", 
								  new Fields("key", "value"));
		Tap out = new StdoutTap();
		Pipe copyPipe = new Pipe("copy");
		FlowDef flowDef = FlowDef.flowDef()
				.addSource(copyPipe, cb)
				.addTailSink(copyPipe, out);
		flowConnector.connect( flowDef ).complete();
	}

}
