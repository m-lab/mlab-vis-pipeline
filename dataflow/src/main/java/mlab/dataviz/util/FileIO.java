package mlab.dataviz.util;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileIO {
	
	/**
	 * Reads a file from a path and returns a string of the body.
	 * @param path String - file path
	 * @return String - contents of file
	 * @throws IOException
	 */
	public static String readFile(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, Charset.defaultCharset());
    }
	
	/**
	 * Writes file to a path
	 * @param path String - path to write to
	 * @param content String - content for the file
	 * @return File path to the new file.
	 * @throws IOException
	 */
	public static Path writeFile(String path, String content) throws IOException {
		 return Files.write(Paths.get(path), content.getBytes());
	}
}
