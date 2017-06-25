package io.github.ouyi.tez;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class HelloWorldTest {
    @Test
    public void run() throws Exception {
        HelloWorld helloWorld = new HelloWorld();
        assertTrue(helloWorld.run(new String[]{"hello", "world"}) == 0);
    }

}