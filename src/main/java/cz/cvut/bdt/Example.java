package cz.cvut.bdt;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.hbase.async.Config;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;

final class Example {

	static final String configFile = "asynchbase.properties";

	public static void main(final String[] args) throws Exception {
		
		//-- PARSE ARGUMENTS --// 
		ArgumentParser parser = new ArgumentParser("HBaseClient");
		parser.addArgument("async_conf", true, true, "specify path to asynchbase.properties");
		parser.addArgument("auth_conf", true, true, "specify path to java.security.auth.login.config");
		parser.addArgument("table", true, true, "specify table name");
		parser.addArgument("row_key", true, true, "specify row key");
		parser.parseAndCheck(args);
		String async_conf = parser.getString("async_conf");
		String auth_conf = parser.getString("auth_conf");
		String table = parser.getString("table");
		String row_key = parser.getString("row_key");
		
		//-- SET java.security.auth.login.config --//
		System.setProperty("java.security.auth.login.config",auth_conf);
		
		
		//-- LOAD DATA FROM HBASE --// 
		Config config = new Config(async_conf);
		HBaseClient client = new HBaseClient(config);
		GetRequest get = new GetRequest(table, row_key);

		try {
			ArrayList<KeyValue> result = client.get(get).joinUninterruptibly();
			System.out.println("Results for key "+row_key+" in table "+table);
			
			for (KeyValue res : result) {
				
				//load doc_id  (string)
				String doc_id = new String(res.qualifier());	//	StandardCharsets.UTF_8
				
				// load tf (integer)
				ByteBuffer bb = ByteBuffer.wrap(res.value());
				int tf = bb.getInt();
				
				// print
				System.out.println(doc_id+" : "+tf);
			}

		} catch (Exception e) {
			System.out.println("Get failed:");
			e.printStackTrace();
		}

		client.shutdown();
	}

}