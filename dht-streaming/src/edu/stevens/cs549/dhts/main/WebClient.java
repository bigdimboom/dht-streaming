package edu.stevens.cs549.dhts.main;

import java.net.URI;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.xml.bind.JAXBElement;

import org.glassfish.jersey.media.sse.EventSource;
import org.glassfish.jersey.media.sse.SseFeature;

import edu.stevens.cs549.dhts.activity.DHTBase;
import edu.stevens.cs549.dhts.activity.NodeInfo;
import edu.stevens.cs549.dhts.activity.DHTBase.Failed;
import edu.stevens.cs549.dhts.resource.TableRep;
import edu.stevens.cs549.dhts.resource.TableRow;

public class WebClient {

	private Logger log = Logger.getLogger(WebClient.class.getCanonicalName());

	private void error(String msg) {
		log.severe(msg);
	}

	/*
	 * Encapsulate Web client operations here.
	 * 
	 * TODO: Fill in missing operations.
	 */

	/*
	 * Creation of client instances is expensive, so just create one.
	 */
	protected Client client;
	
	protected Client listenClient;

	public WebClient() {
		client = ClientBuilder.newClient();
		listenClient = ClientBuilder.newBuilder().register(SseFeature.class).build();	
	}

	private void info(String mesg) {
		Log.info(mesg);
	}

	private Response getRequest(URI uri) {
		try {
			Response cr = client.target(uri)
					.request(MediaType.APPLICATION_XML_TYPE)
					.header(Time.TIME_STAMP, Time.advanceTime())
					.get();
			processResponseTimestamp(cr);
			return cr;
		} catch (Exception e) {
			error("Exception during GET request: " + e);
			return null;
		}
	}

	private Response putRequest(URI uri, Entity<?> entity) {
		try {
			Response cr = client.target(uri)
					.request()
					.header(Time.TIME_STAMP, Time.advanceTime())
					.put(entity);
			processResponseTimestamp(cr);
			return cr;
		} catch (Exception e) {
			error("Exception during PUT request: " + e);
			return null;
		}
		//Done
	}
	
	private Response deleteRequest(URI uri) {
		try {
			Response cr = client.target(uri)
					.request()
					.header(Time.TIME_STAMP, Time.advanceTime())
					.delete();
			processResponseTimestamp(cr);
			return cr;
		} catch (Exception e) {
			error("Exception during DELETE request: " + e);
			return null;
		}
		//Done
	}
	
	
	@SuppressWarnings("unused")
	private Response putRequest(URI uri) {
		return putRequest(uri, Entity.text(""));
	}

	private void processResponseTimestamp(Response cr) {
		Time.advanceTime(Long.parseLong(cr.getHeaders().getFirst(Time.TIME_STAMP).toString()));
	}

	/*
	 * Jersey way of dealing with JAXB client-side: wrap with run-time type
	 * information.
	 */
	private GenericType<JAXBElement<NodeInfo>> nodeInfoType = new GenericType<JAXBElement<NodeInfo>>() {
	};
	
	//Done
	private GenericType<JAXBElement<TableRow>> tableRowType = new GenericType<JAXBElement<TableRow>>() {
	};

	/*
	 * Ping a remote site to see if it is still available.
	 */
	public boolean isFailed(URI base) {
		URI uri = UriBuilder.fromUri(base).path("info").build();
		Response c = getRequest(uri);
		return c.getStatus() >= 300;
	}

	/*
	 * Get the predecessor pointer at a node.
	 */
	public NodeInfo getPred(NodeInfo node) throws DHTBase.Failed {
		URI predPath = UriBuilder.fromUri(node.addr).path("pred").build();
		info("client getPred(" + predPath + ")");
		Response response = getRequest(predPath);
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /pred");
		} else {
			NodeInfo pred = response.readEntity(nodeInfoType).getValue();
			return pred;
		}
	}

	/*
	 * Get the successor pointer at a node. //Done
	 */
	public NodeInfo getSucc(NodeInfo node) throws DHTBase.Failed {
		URI succPath = UriBuilder.fromUri(node.addr).path("succ").build();
		info("client getSucc(" + succPath + ")");
		Response response = getRequest(succPath);
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /succ");
		} else {
			NodeInfo succ = response.readEntity(nodeInfoType).getValue();
			return succ;
		}
	}
	
	/*
	 * Get the successor pointer of an id //Done
	 */
	public NodeInfo findSuccessor(URI addr, int id) throws DHTBase.Failed {
		UriBuilder ub = UriBuilder.fromUri(addr).path("find");
		URI findSuccPath = ub.queryParam("id", id).build();
		info("client findSucc(" + findSuccPath + ")");
		Response response = getRequest(findSuccPath);
		
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /find?id=ID");
		} else {
			return (NodeInfo) response.readEntity(nodeInfoType).getValue();
		}
	}
	
	/*
	 * Get the closestPrecedingFinger pointer of an id //Done
	 */
	public NodeInfo closestPrecedingFinger(URI addr, int id) throws DHTBase.Failed {
		UriBuilder ub = UriBuilder.fromUri(addr).path("finger");
		URI getPredPath = ub.queryParam("id", id).build();
		info("client getPredFinger(" + getPredPath + ")");
		Response response = getRequest(getPredPath);
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /finger?id=ID");
		} else {
			return (NodeInfo) response.readEntity(nodeInfoType).getValue();
		}
	}
	
	//client Restful get query :a value array 
	public String[] get(URI addr, String k) throws Failed {
		UriBuilder ub = UriBuilder.fromUri(addr);
		URI getPath = ub.queryParam("key", k).build();
		info("client get(" + getPath + ")");
		Response response = getRequest(getPath);
		TableRow tableRow;
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("GET ?key=KEY");
		} else {
			tableRow = response.readEntity(tableRowType).getValue();
		}
		
		if(tableRow == null)
		{
			String[] ret = {"NULL"};
			return ret;
		}
		
		return tableRow.vals;
		//Done
	}

	//client Restful add key/val query
	public void add(URI addr, String k, String v) throws Failed {
		UriBuilder ub = UriBuilder.fromUri(addr);
		URI putKeyPath = ub.queryParam("key", k).queryParam("val", v).build();
		info("client add(" + putKeyPath + ")");
		Response response = putRequest(putKeyPath, Entity.text(v));
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("PUT ?key=KEY&val=VAL");
		}
		//Done
	}

	//client Restful delete queryinfo("I get the values: " + values);
	public void delete(URI addr, String k, String v) throws Failed {
		UriBuilder ub = UriBuilder.fromUri(addr);
		URI deleteKeyPath = ub.queryParam("key", k).queryParam("val", v).build();
		info("client delete(" + deleteKeyPath + ")");
		Response response = deleteRequest(deleteKeyPath);
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("DELETE ?key=KEY&val=VAL");
		}
		//Done
	}
	
	/*
	 * Notify node that we (think we) are its predecessor.
	 */
	public TableRep notify(NodeInfo node, TableRep predDb) throws DHTBase.Failed {
		/*
		 * The protocol here is more complex than for other operations. We
		 * notify a new successor that we are its predecessor, and expect its
		 * bindings as a result. But if it fails to accept us as its predecessor
		 * (someone else has become intermediate predecessor since we found out
		 * this node is our successor i.e. race condition that we don't try to
		 * avoid because to do so is infeasible), it notifies us by returning
		 * null. This is represented in HTTP by RC=304 (Not Modified).
		 */
		NodeInfo thisNode = predDb.getInfo();
		UriBuilder ub = UriBuilder.fromUri(node.addr).path("notify");
		URI notifyPath = ub.queryParam("id", thisNode.id).build();
		info("client notify(" + notifyPath + ")");
		Response response = putRequest(notifyPath, Entity.xml(predDb));
		if (response != null && response.getStatusInfo() == Response.Status.NOT_MODIFIED) {
			/*
			 * Do nothing, the successor did not accept us as its predecessor.
			 */
			return null;
		} else if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("PUT /notify?id=ID");
		} else {
			TableRep bindings = response.readEntity(TableRep.class);
			return bindings;
		}
	}
	
	public EventSource listenForBindings(NodeInfo node, int id, String skey) throws DHTBase.Failed {
		// TODO listen for SSE subscription requests on http://.../dht/listen?key=<key>
		// On the service side, don't expect LT request or response headers for this request.
		// Note: "id" is client's id, to enable us to stop event generation at the server.
		UriBuilder ub = UriBuilder.fromUri(node.addr).path("listen");
		URI path = ub.queryParam("id", id).queryParam("key", skey).build();
//		Response response = getRequestForListeners(path);
//		if (response == null || response.getStatus() >= 300) {
//			throw new DHTBase.Failed("GET /listen?id=ID&key=KEY");
//		}
		WebTarget webTarget = listenClient.target(path);
		EventSource eventSource = new EventSource(webTarget);
		return eventSource;
		//Done
	}

	public void listenOff(NodeInfo node, int id, String skey) throws DHTBase.Failed {
		// TODO listen for SSE subscription requests on http://.../dht/listen?key=<key>
		// On the service side, don't expect LT request or response headers for this request.
		UriBuilder ub = UriBuilder.fromUri(node.addr).path("listen");
		URI path = ub.queryParam("id", id).queryParam("key", skey).build();
		//deleteRequestForListeners(path);
		//DONE
	}

}
