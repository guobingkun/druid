package io.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Interval;

/**
 */
public class ClientHadoopReindexQuery
{
  private final String dataSource;
  private final Interval intervalToReindex;

  @JsonCreator
  public ClientHadoopReindexQuery(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval intervalToReindex
  )
  {
    this.dataSource = dataSource;
    this.intervalToReindex = intervalToReindex;
  }

  @JsonProperty
  public String getType()
  {
    return "reindex_hadoop";
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Interval getIntervalToReindex()
  {
    return intervalToReindex;
  }

  @Override
  public String toString()
  {
    return "ClientHadoopMergeQuery{" +
           "dataSource='" + dataSource + '\'' +
           ", intervalToReindex=" + intervalToReindex +
           '}';
  }
}
