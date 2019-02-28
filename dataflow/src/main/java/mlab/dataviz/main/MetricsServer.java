package mlab.dataviz.main;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;

/**
 * Initializes a prometheus metrics server collection endpoint.
 * @author iros
 */
public class MetricsServer implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(MetricsServer.class);
	public MetricsServer() {
	}
	
	@SuppressWarnings("serial")
	public class HTTPRootHandler extends HttpServlet {
		@Override
	    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
	        throws ServletException, IOException {
	      resp.getWriter().println("Hello World!");
	     
	    }
	}

	@Override
	public void run() {

		Server server = new Server(new InetSocketAddress(9090));
		@SuppressWarnings("resource")
		ServerConnector connector = new ServerConnector(server);

		connector.setPort(9090);
		connector.setHost("localhost");

		ServletContextHandler context = new ServletContextHandler();
		context.setContextPath("/");
		server.setHandler(context);

		// Add root servlet (outputs hello world.)
		context.addServlet(new ServletHolder(new HTTPRootHandler()), "/");
		
		// Add metrics servlet
		context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");

		// Add metrics about CPU, JVM memory etc.
		DefaultExports.initialize();

		try {
			// Start the web server.
			server.start();
			server.join();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.getLocalizedMessage());
		}
	}

}
