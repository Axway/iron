package io.axway.iron.core.spi.file;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.atomic.*;

public final class AtomicBigInteger {

    private final AtomicReference<BigInteger> m_bigInteger;

    public AtomicBigInteger(final BigInteger bigInteger) {
        m_bigInteger = new AtomicReference<>(Objects.requireNonNull(bigInteger));
    }

    public BigInteger incrementAndGet() {
        return m_bigInteger.accumulateAndGet(BigInteger.ONE, BigInteger::add);
    }

    public BigInteger getAndIncrement() {
        return m_bigInteger.getAndAccumulate(BigInteger.ONE, BigInteger::add);
    }

    public BigInteger get() {
        return m_bigInteger.get();
    }
}
