package io.axway.iron.core.spi.file;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.atomic.*;

public final class AtomicBigInteger {

    private final AtomicReference<BigInteger> bigInteger;

    public AtomicBigInteger(final BigInteger bigInteger) {
        this.bigInteger = new AtomicReference<>(Objects.requireNonNull(bigInteger));
    }

    // Method references left out for demonstration purposes
    public BigInteger incrementAndGet() {
        return bigInteger.accumulateAndGet(BigInteger.ONE, BigInteger::add);
    }

    public BigInteger getAndIncrement() {
        return bigInteger.getAndAccumulate(BigInteger.ONE, BigInteger::add);
    }

    public BigInteger get() {
        return bigInteger.get();
    }
}
