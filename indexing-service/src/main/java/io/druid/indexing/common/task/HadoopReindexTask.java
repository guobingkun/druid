package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.metamx.common.Granularity;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.granularity.QueryGranularity;
import io.druid.indexer.HadoopIOConfig;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexer.HadoopTuningConfig;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.LockAcquireAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.hadoop.OverlordActionBasedUsedSegmentLister;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class HadoopReindexTask extends HadoopTask
{

  private static final Logger log = new Logger(HadoopReindexTask.class);

  @JsonIgnore
  private final ObjectMapper jsonMapper;

  private final Interval interval;

  private HadoopIngestionSpec spec;

  @JsonCreator
  public HadoopReindexTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("hadoopDependencyCoordinates") List<String> hadoopDependencyCoordinates,
      @JsonProperty("metricsSpec") AggregatorFactory[] aggregators,
      @JsonProperty("interval") Interval interval,
      @JacksonInject ObjectMapper jsonMapper,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        id != null ? id : String.format("index_hadoop_%s_%s", dataSource, new DateTime()),
        dataSource,
        hadoopDependencyCoordinates,
        context
    );
    this.interval = interval;
    this.jsonMapper = jsonMapper;

    this.spec = new HadoopIngestionSpec(
        new DataSchema(
            dataSource,
            null,
            aggregators,
            new UniformGranularitySpec(
                Granularity.HOUR,
                QueryGranularity.NONE,
                ImmutableList.of(interval)
            ),
            null
        ),
        new HadoopIOConfig(ImmutableMap.<String, Object>builder().build(), null, null),
        new HadoopTuningConfig()
    );

  }

  @Override
  public String getType()
  {
    return "reindex_hadoop";
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return true;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    final ClassLoader loader = buildClassLoader(toolbox);
    boolean determineIntervals = !spec.getDataSchema().getGranularitySpec().bucketIntervals().isPresent();

    spec = HadoopIngestionSpec.updateSegmentListIfDatasourcePathSpecIsUsed(
        spec,
        jsonMapper,
        new OverlordActionBasedUsedSegmentLister(toolbox)
    );

    final String config = invokeForeignLoader(
        "io.druid.indexing.common.task.HadoopIndexTask$HadoopDetermineConfigInnerProcessing",
        new String[]{
            toolbox.getObjectMapper().writeValueAsString(spec),
            toolbox.getConfig().getHadoopWorkingPath(),
            toolbox.getSegmentPusher().getPathForHadoop(getDataSource())
        },
        loader
    );

    final HadoopIngestionSpec indexerSchema = toolbox
        .getObjectMapper()
        .readValue(config, HadoopIngestionSpec.class);


    // We should have a lock from before we started running only if interval was specified
    final String version;
    if (determineIntervals) {
      Interval interval = JodaUtils.umbrellaInterval(
          JodaUtils.condenseIntervals(
              indexerSchema.getDataSchema().getGranularitySpec().bucketIntervals().get()
          )
      );
      TaskLock lock = toolbox.getTaskActionClient().submit(new LockAcquireAction(interval));
      version = lock.getVersion();
    } else {
      Iterable<TaskLock> locks = getTaskLocks(toolbox);
      final TaskLock myLock = Iterables.getOnlyElement(locks);
      version = myLock.getVersion();
    }

    log.info("Setting version to: %s", version);

    final String segments = invokeForeignLoader(
        "io.druid.indexing.common.task.HadoopIndexTask$HadoopIndexGeneratorInnerProcessing",
        new String[]{
            toolbox.getObjectMapper().writeValueAsString(indexerSchema),
            version
        },
        loader
    );

    if (segments != null) {
      List<DataSegment> publishedSegments = toolbox.getObjectMapper().readValue(
          segments,
          new TypeReference<List<DataSegment>>()
          {
          }
      );

      toolbox.pushSegments(publishedSegments);
      return TaskStatus.success(getId());
    } else {
      return TaskStatus.failure(getId());
    }
  }

  @JsonProperty
  public List<String> getHadoopDependencyCoordinates()
  {
    return super.getHadoopDependencyCoordinates();
  }

}
