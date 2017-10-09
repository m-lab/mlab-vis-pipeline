package mlab.dataviz;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import mlab.dataviz.pipelineopts.AlivePingOptions;

/**
 * Alive Prometheus test.
 * This is used to ping the prometheus server, as a signal of life.
 * Required parameters are:
 * --prometheus=<instance of prometheus to hit>
 * 
 * Should we run with service key via GOOGLE_APPLICATION_CREDENTIALS.
 */
public class AlivePing {

	private static final Logger LOG = LoggerFactory.getLogger(AlivePing.class);
	
    public static void main(String[] args) {
    	 CollectorRegistry registry = new CollectorRegistry();
    	 Gauge duration = Gauge.build()
    		     .name("mlab_vis_pipeline_alive_ping")
    		     .help("Duration of test container in seconds.")
    		     .register(registry);
    	 
    	 Gauge.Timer durationTimer = duration.startTimer();
    	 
    	 PipelineOptionsFactory.register(AlivePingOptions.class);
    	 AlivePingOptions options = PipelineOptionsFactory.fromArgs(args)
 				.withValidation()
 				.as(AlivePingOptions.class);
 		
    	 System.out.println("App alive, notifying " + options.getPrometheus());
   	   
    	 durationTimer.setDuration();
     PushGateway pg = new PushGateway(options.getPrometheus());
     try {
    	 	pg.pushAdd(registry, "mlab_vis_pipeline");
     } catch (IOException e) {
    	 	e.printStackTrace();
    	 	LOG.error(e.getMessage());
    	 	LOG.error(e.getStackTrace().toString());
     }
   }
}