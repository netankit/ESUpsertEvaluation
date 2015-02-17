package com.teradata.upserteval;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

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
		ArrayList<String> names = new ArrayList<String>();

		/*
		 * Command Line arguments
		 */
		if (args.length != 3) {
			System.out
					.println("java -jar ContinousUpsertEvaluation <num_of_documents> <index_name> <type_name>");
			System.exit(0);
		}

		long num_of_documents = Long.parseLong(args[0]);
		String index_name = args[1];
		String type_name = args[2];

		Logger log = Logger
				.getLogger(ContinousUpsertEvaluation.class.getName());
		FileHandler fh;
		fh = new FileHandler("ContinousUpsertsLogFile.log");
		log.addHandler(fh);
		SimpleFormatter formatter = new SimpleFormatter();
		fh.setFormatter(formatter);

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
			// System.out.println("Document Number: " + i);

			// Recording Start Time
			long startTime = System.currentTimeMillis();

			String randomID = Integer.toString(getRandomNumber(1, 100000));

			IndexRequest indexRequest = new IndexRequest(index_name, type_name,
					randomID).source(XContentFactory.jsonBuilder()
					.startObject().field("name", getRandomName(names))
					.field("gender", getRandomGender())
					.field("city", getRandomCity())
					.field("age", getRandomNumber(10, 95))
					.field("telephoneNumber", getRandomNumber(990000, 999999))
					.endObject());

			UpdateRequest updateRequest = new UpdateRequest(index_name,
					type_name, randomID).doc(
					XContentFactory.jsonBuilder().startObject()
							.field("gender", getRandomGender())
							.field("city", getRandomCity()).endObject())
					.upsert(indexRequest);

			updateResponse = client.update(updateRequest).get();

			// Recording Stop Time
			long stopTime = System.currentTimeMillis();
			long totalElapsedTime = stopTime - startTime;
			// System.out.println("Iteration: " + i + "Total Time Taken (ms): "
			// + totalElapsedTime);

			log.info("Iteration: " + i + "\tElapsed Time:" + totalElapsedTime);

		}
	}

	private static String getRandomGender() {

		int max = 1;
		int min = 0;
		Random rand = new Random();

		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive
		int randomNum = rand.nextInt((max - min) + 1) + min;

		if (randomNum == 0) {
			return "male";
		} else {
			return "female";
		}

	}

	private static int getRandomNumber(int min, int max) {

		// Usually this should be a field rather than a method variable so
		// that it is not re-seeded every call.
		Random rand = new Random();

		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive
		int randomNum = rand.nextInt((max - min) + 1) + min;

		return randomNum;
	}

	private static String getRandomCity() {

		String[] cityList = { "MUNICH", "FRANKFURT", "DELHI", "MUMBAI",
				"SAN FRANCISCO", "BERLIN", "SEATTLE", "NEW YORK", "VENICE",
				"HAMBURG" };
		// Usually this should be a field rather than a method variable so
		// that it is not re-seeded every call.
		Random rand = new Random();

		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive
		int randomNum = rand.nextInt(cityList.length);

		return cityList[randomNum];
	}

	private static String getRandomName(ArrayList<String> names) {
		if (names.size() == 0) {
			try {
				// Open the file that is the first
				// command line parameter
				FileInputStream fstream = new FileInputStream(
						"./src/main/resources/all_names.txt");
				// Get the object of DataInputStream
				DataInputStream in = new DataInputStream(fstream);
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String strLine;
				// Read File Line By Line
				while ((strLine = br.readLine()) != null) {
					// Print the content on the console
					names.add(strLine);
				}
				// Close the input stream
				in.close();
			} catch (Exception e) {// Catch exception if any
				System.err.println("Error: " + e.getMessage());
			}
		}
		int index = new Random().nextInt(names.size());
		String random_name = names.get(index);
		return random_name;
	}
}
