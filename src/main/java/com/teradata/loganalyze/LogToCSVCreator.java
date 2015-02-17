package com.teradata.loganalyze;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class LogToCSVCreator {
	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.out.println("java -jar LogToCSVCreator <logFileName>");
			System.exit(0);
		}

		String logFileName = args[0];
		// The name of the file to open.
		String fileNameRead = logFileName + ".log";

		String fileNameWrite = logFileName + ".csv";

		// This will reference one line at a time
		String line = null;

		// FileReader reads text files in the default encoding.
		FileReader fileReader;
		try {
			fileReader = new FileReader(fileNameRead);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			// Assume default encoding.
			FileWriter fileWriter = new FileWriter(fileNameWrite);

			// Always wrap FileWriter in BufferedWriter.
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

			while ((line = bufferedReader.readLine()) != null) {
				// System.out.println(line);
				if (line.startsWith("INFO:")) {
					String line_1 = line.replace("INFO: Iteration: ", "");
					String line_2 = line_1.replace("Elapsed Time:", "");
					String[] val = line_2.split("\t");
					bufferedWriter.write(val[0] + "," + val[1] + "\n");

				}

			}

			// Always wrap FileReader in BufferedReader.
			// Always close files.
			bufferedReader.close();
			bufferedWriter.close();
			System.out.println("Done");

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
