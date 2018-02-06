package io.axway.iron.core.spi.file;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.atomic.*;

import static java.math.BigInteger.ONE;

public final class AtomicBigInteger {

    private final AtomicReference<BigInteger> m_bigInteger;

    public AtomicBigInteger(BigInteger bigInteger) {
        m_bigInteger = new AtomicReference<>(Objects.requireNonNull(bigInteger));
    }

    public BigInteger incrementAndGet() {
        return m_bigInteger.accumulateAndGet(ONE, BigInteger::add);
    }

    public BigInteger getAndIncrement() {
        return m_bigInteger.getAndAccumulate(ONE, BigInteger::add);
    }

    public BigInteger get() {
        return m_bigInteger.get();
    }
}
