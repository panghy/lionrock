package io.github.panghy.lionrock.foundationdb;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "lionrock.foundationdb")
public class Configuration {

  /**
   * The FDB version to use when initializing FDB.
   */
  private int fdbVersion = 630;

  /**
   * Controls the default fdb timeout (when no gRPC deadline is present).
   */
  private long defaultFdbTimeoutMs = 60_000;

  private List<Cluster> clusters = new ArrayList<>();

  /**
   * Internal options (mainly for testing).
   */
  private InternalOptions internal = new InternalOptions();

  public InternalOptions getInternal() {
    return internal;
  }

  public void setInternal(InternalOptions internal) {
    this.internal = internal;
  }

  public int getFdbVersion() {
    return fdbVersion;
  }

  public void setFdbVersion(int fdbVersion) {
    this.fdbVersion = fdbVersion;
  }

  public List<Cluster> getClusters() {
    return clusters;
  }

  public void setClusters(List<Cluster> clusters) {
    this.clusters = clusters;
  }

  public long getDefaultFdbTimeoutMs() {
    return defaultFdbTimeoutMs;
  }

  public void setDefaultFdbTimeoutMs(long defaultFdbTimeoutMs) {
    this.defaultFdbTimeoutMs = defaultFdbTimeoutMs;
  }

  public static class Cluster {
    private String clusterFile;
    private String name;

    public String getClusterFile() {
      return clusterFile;
    }

    public String getName() {
      return name;
    }

    public void setClusterFile(String clusterFile) {
      this.clusterFile = clusterFile;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  /**
   * Internal options to simulate different behaviors.
   */
  public static class InternalOptions {
    /**
     * Whether getRange requests are internally translated to asList() async calls and returned as a single batch.
     */
    private boolean useAsListForRangeGets = false;
    /**
     * Whether getRange requests that are internally translated as a single asList() async calls would be re-partitioned
     * again as multiple response objects.
     */
    private boolean simulatePartitionsForAsListRangeGets = false;
    /**
     * If {@link #simulatePartitionsForAsListRangeGets} is enabled, the max size of each partition.
     */
    private int partitionSizeForAsListRangeGets = 50;

    public boolean isUseAsListForRangeGets() {
      return useAsListForRangeGets;
    }

    public void setUseAsListForRangeGets(boolean useAsListForRangeGets) {
      this.useAsListForRangeGets = useAsListForRangeGets;
    }

    public boolean isSimulatePartitionsForAsListRangeGets() {
      return simulatePartitionsForAsListRangeGets;
    }

    public void setSimulatePartitionsForAsListRangeGets(boolean simulatePartitionsForAsListRangeGets) {
      this.simulatePartitionsForAsListRangeGets = simulatePartitionsForAsListRangeGets;
    }

    public int getPartitionSizeForAsListRangeGets() {
      return partitionSizeForAsListRangeGets;
    }

    public void setPartitionSizeForAsListRangeGets(int partitionSizeForAsListRangeGets) {
      this.partitionSizeForAsListRangeGets = partitionSizeForAsListRangeGets;
    }
  }
}
