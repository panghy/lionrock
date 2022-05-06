package io.github.panghy.lionrock.inmemory;

import com.google.common.primitives.UnsignedBytes;

import java.util.Arrays;

/**
 * Wraps byte-arrays and provides comparisons.
 *
 * @author Clement Pang
 */
public class BytesValue implements Comparable<BytesValue> {

  private final byte[] value;

  public BytesValue(byte[] value) {
    this.value = value;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public int compareTo(BytesValue o) {
    return UnsignedBytes.lexicographicalComparator().compare(value, o.value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BytesValue that = (BytesValue) o;
    return Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(value);
  }
}
