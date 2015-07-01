import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;

public class VirtuosoSparqlConnector {
	private String host;
	private String  format = "text/plain";
    private int timeout = 0;
    
    private boolean debug = true;
	// ctor
	
    public VirtuosoSparqlConnector(String host){
		this.host = host;
	};

	public String request(String query) throws Exception {
		String url = "http://" + host + "/webapi/rdf/sparql?";
		
		List<BasicNameValuePair> params = new LinkedList<BasicNameValuePair>();
		params.add(new BasicNameValuePair("default-graph-uri", ""));
		params.add(new BasicNameValuePair("query", query));
		params.add(new BasicNameValuePair("format", format));
		params.add(new BasicNameValuePair("timeout", String.valueOf(timeout)));
		params.add(new BasicNameValuePair("debug", debug == true ? "on" : "off"));	
		
		String paramString = URLEncodedUtils.format(params, "utf-8");
		url += paramString;
		
		System.out.println("+++ " + url);
		
		String response = null;
		HttpURLConnection connection = null;
		try {
			// set up html connection object
			connection = (HttpURLConnection) new URL(url).openConnection();
			connection.setRequestMethod("GET");
			connection.setDoOutput(true);

			// build DOM object
			InputStream inputStream = connection.getInputStream();
			String encoding = connection.getContentEncoding();
			encoding = encoding == null ? "UTF-8" : encoding;
			response = IOUtils.toString(inputStream, encoding);
		
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			//close the connection
			connection.disconnect();
		}
		return response;		
	}
	
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}
}