package io.axway.iron.spi;

import java.io.*;
import java.util.*;

import static io.axway.alf.assertion.Assertion.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

public class StoreNamePrefixManagement {
    private final Map<String, byte[]> m_storePrefixes = new WeakHashMap<>();

    public void writeNamePrefix(String storeName, OutputStream os) throws IOException {
        checkState(!storeName.isEmpty(), "Store name can't be null or empty");

        byte[] prefix;
        synchronized (m_storePrefixes) {
            prefix = m_storePrefixes.get(storeName);
            if (prefix == null) {
                byte[] storeKey = encodeStoreName(storeName);
                prefix = new byte[Integer.BYTES + storeKey.length];
                encodeInt(prefix, 0, storeKey.length);
                System.arraycopy(storeKey, 0, prefix, Integer.BYTES, storeKey.length);
                m_storePrefixes.put(storeName, prefix);
            }
        }

        os.write(prefix);
    }

    public static String readStoreName(InputStream is) throws IOException {
        int storeKeyLength = decodeInt(is);
        byte[] nameBytes = new byte[storeKeyLength];
        int nbRead = is.read(nameBytes);
        checkState(nbRead == storeKeyLength, "Unexpected end of stream, could not read store key");
        return decodeStoreName(nameBytes);
    }

    private static byte[] encodeStoreName(String storeName) {
        return storeName.getBytes(UTF_8);
    }

    private static String decodeStoreName(byte[] encoded) {
        return new String(encoded, UTF_8);
    }

    private static void encodeInt(byte[] data, int pos, int i) {
        int decl = Integer.SIZE;
        int newPos = pos;
        while (decl > 0) {
            decl -= Byte.SIZE;
            data[newPos++] = (byte) (i >> decl);
        }
    }

    private static int decodeInt(InputStream is) throws IOException {
        int i = 0;

        int bytes = Integer.BYTES;
        while (bytes-- > 0) {
            i <<= Byte.SIZE;
            i |= (((byte) is.read()) & 0xFF);
        }

        return i;
    }
}
