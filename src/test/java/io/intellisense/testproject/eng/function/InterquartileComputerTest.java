package io.intellisense.testproject.eng.function;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InterquartileComputerTest {

    private final InterquartileComputer iqrComputer = new InterquartileComputer();

    @Test
    void getScore() {
        assertEquals(0, iqrComputer.getScore(1, 1));
        assertEquals(0.5, iqrComputer.getScore(5, 5));
        assertEquals(1, iqrComputer.getScore(14, 4));
    }
}