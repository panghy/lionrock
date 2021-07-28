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
}
