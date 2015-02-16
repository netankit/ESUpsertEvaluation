package com.teradata.upserteval;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;

public class ContinousUpsertEvaluation {
	private static UpdateResponse updateResponse;

	/**
	 * 
	 * @param args
	 *            [0]: Number of Documents
	 * @param args
	 *            [1]: Index name
	 * @param args
	 *            [2]: Type Name
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ExecutionException {

		/*
		 * Command Line arguments
		 */

		long num_of_documents = Long.parseLong(args[0]);
		String index_name = args[1];
		String type_name = args[2];

		if (args.length != 3) {
			System.out
					.println("java -jar ContinousUpsertEvaluation <num_of_documents> <index_name> <type_name>");
			System.exit(0);
		}

		/*
		 * ES node and client initialization.
		 */
		Node node = nodeBuilder().node();
		Client client = node.client();

		/*
		 * Index Schema: For Upsert Evaluation
		 * 
		 * @name
		 * 
		 * @gender
		 * 
		 * @city
		 * 
		 * @age
		 * 
		 * @telephoneNumber
		 */

		System.out.println("Starting Upsert Evaluation....");
		for (int i = 0; i < num_of_documents; i++) {
			System.out.println("Document Number: " + i);

			// Recording Start Time
			long startTime = System.currentTimeMillis();

			IndexRequest indexRequest = new IndexRequest(index_name, type_name,
					"1").source(XContentFactory.jsonBuilder().startObject()
					.field("name", "Ankit Bahuguna").field("gender", "male")
					.field("city", "Munich").field("age", "24")
					.field("telephoneNumber", "12345678910").endObject());

			UpdateRequest updateRequest = new UpdateRequest(index_name,
					type_name, "1").doc(
					XContentFactory.jsonBuilder().startObject()
							.field("gender", "male").endObject()).upsert(
					indexRequest);

			updateResponse = client.update(updateRequest).get();

			// Recording Stop Time
			long stopTime = System.currentTimeMillis();
			long totalTime = stopTime - startTime;
			System.out.println("Total Time Taken (ms): " + totalTime);
		}
	}
}
