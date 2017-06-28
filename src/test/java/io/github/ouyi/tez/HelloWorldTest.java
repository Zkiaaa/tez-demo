package io.github.ouyi.tez;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class HelloWorldTest {
    @Test
    public void run() throws Exception {
        HelloWorld helloWorld = new HelloWorld();
        String input = "src/test/resources/input.txt";
        String output = "build/" + HelloWorldTest.class.getSimpleName() + "/output";
        String localMode = "true";
        assertTrue(helloWorld.run(new String[]{input, output, localMode}) == 0);
    }

}