package io.github.ouyi.tez;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class HelloWorldTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void run() throws Exception {
        HelloWorld helloWorld = new HelloWorld();
        String input = "src/test/resources/input.txt";
        String output = temporaryFolder.newFolder().getPath();
        String localMode = "true";
        assertTrue(helloWorld.run(new String[]{input, output, localMode}) == 0);

        String expectedOutput = "src/test/resources/output.tsv";
        byte[] expected = Files.readAllBytes(Paths.get(expectedOutput));
        byte[] actual = Files.readAllBytes(Paths.get(output, "part-v001-o000-r-00000"));
        assertArrayEquals(expected, actual);
    }

}