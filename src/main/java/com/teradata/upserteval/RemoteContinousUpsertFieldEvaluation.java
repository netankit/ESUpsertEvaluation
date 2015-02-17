package com.teradata.upserteval;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.commons.lang.RandomStringUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;

public class RemoteContinousUpsertFieldEvaluation {

	public static void main(String[] args) throws SecurityException,
			IOException, InterruptedException, ExecutionException {

		/*
		 * Command Line arguments
		 */

		if (args.length != 7) {
			System.out
					.println("java -jar RemoteContinousUpsertFieldEvaluation <ESHOST_NAME> <ES_CLUSERNAME> <num_of_iterations> <index_name> <type_name> <logFileName> <num_of_fields>");
			System.exit(0);
		}
		String ESHOST_NAME = args[0];
		String ESCLUSTER_NAME = args[1];
		long num_of_iterations = Long.parseLong(args[2]);
		String index_name = args[3];
		String type_name = args[4];
		String logFileName = args[5];
		int num_of_fields = Integer.parseInt(args[6]);

		Logger log = Logger
				.getLogger(ContinousUpsertEvaluation.class.getName());
		FileHandler fh;
		fh = new FileHandler(logFileName);
		log.addHandler(fh);
		SimpleFormatter formatter = new SimpleFormatter();
		fh.setFormatter(formatter);

		/*
		 * ES node and client initialization.
		 */
		Node node = nodeBuilder().node();

		// Connects to Remote Client defined by the ESHOST_NAME and Cluster
		// defined by ESCLUSTER_NAME

		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", ESCLUSTER_NAME).build();
		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(
						ESHOST_NAME, 9300));

		// Connects to a locally installed ES Instance.
		// Client client = node.client();

		String RANDOM_ID = "1";

		System.out.println("Starting Upsert Evaluation....");
		for (int i = 0; i < num_of_iterations; i++) {
			// System.out.println("Document Number: " + i);

			// Recording Start Time
			long startTime = System.currentTimeMillis();

			IndexRequest indexRequest = new IndexRequest(index_name, type_name,
					RANDOM_ID).source(XContentFactory
					.jsonBuilder()
					.startObject()
					.field(getRandomFieldName(num_of_fields),
							RandomStringUtils.randomAlphabetic(15))
					.field(getRandomFieldName(num_of_fields),
							RandomStringUtils.randomNumeric(8)).endObject());

			UpdateRequest updateRequest = new UpdateRequest(index_name,
					type_name, RANDOM_ID).doc(
					XContentFactory
							.jsonBuilder()
							.startObject()
							.field(getRandomFieldName(num_of_fields),
									RandomStringUtils.randomAlphabetic(12))
							.endObject()).upsert(indexRequest);

			UpdateResponse updateResponse = client.update(updateRequest).get();

			// Recording Stop Time
			long stopTime = System.currentTimeMillis();
			long totalElapsedTime = stopTime - startTime;
			// System.out.println("Iteration: " + i + "Total Time Taken (ms): "
			// + totalElapsedTime);

			log.info("Iteration: " + i + "\tElapsed Time:" + totalElapsedTime);

		}
		// Closes the Remote Client on finishing the job.
		client.close();

	}

	private static String getRandomFieldName(int num_of_fields) {

		int min = 1;
		int max = num_of_fields;

		// Usually this should be a field rather than a method variable so
		// that it is not re-seeded every call.
		Random rand = new Random();

		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive
		int randomNum = rand.nextInt((max - min) + 1) + min;

		return "field" + randomNum;
	}

}
