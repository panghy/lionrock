package io.github.panghy.lionrock.inmemory;

import org.bouncycastle.util.Arrays;

/**
 * Caches hash code for {@link BytesValue}
 *
 * @author Clement Pang
 */
public class BytesKey extends BytesValue {

  private final int hashCode;

  public BytesKey(byte[] value) {
    super(value);
    this.hashCode = Arrays.hashCode(value);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }
}
