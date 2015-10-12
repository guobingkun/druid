package io.druid.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metamx.common.Pair;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.coordinator.helper.DruidCoordinatorHadoopSegmentMerger;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.SingleDimensionShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class DruidCoordinatorHadoopSegmentMergerTest
{
  private static final long mergeBytesLimit = 100;

  @Test
  public void testNoMerges()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(100).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(100).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(100).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(100).build()
    );

    Assert.assertEquals(
        ImmutableList.of(),
        merge(segments)
    );
  }

  @Test
  public void testMergeAtStart()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(100).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(100).build()
    );

    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", new Interval("2012-01-01/P2D"))),
        merge(segments)
    );
  }

  @Test
  public void testMergeAtEnd()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(100).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(100).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(20).build()
    );

    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", new Interval("2012-01-03/P2D"))),
        merge(segments)
    );
  }

  @Test
  public void testMergeInMiddle()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(100).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(100).build()
    );

    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", new Interval("2012-01-02/P2D"))),
        merge(segments)
    );
  }

  @Test
  public void testMergeNoncontiguous()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(10).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(70).build()
    );

    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", new Interval("2012-01-01/2012-01-05"))),
        merge(segments)
    );
  }

  @Test
  public void testMergeSeries()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(50).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(50).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(50).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(50).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("2").size(50).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(50).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of("foo", new Interval("2012-01-01/P2D")),
            Pair.of("foo", new Interval("2012-01-03/P2D")),
            Pair.of("foo", new Interval("2012-01-05/P2d"))
        ),
        merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge1()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(50).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P4D")).version("2").size(30).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("3").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("4").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("3").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-07/P1D")).version("2").size(40).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of("foo", new Interval("2012-01-01/2012-01-04")),
            Pair.of("foo", new Interval("2012-01-04/2012-01-08"))
        ),
        merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge2()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P8D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("3").size(8).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("3").size(8).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-09/P1D")).version("3").size(8).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-10/P1D")).version("3").size(8).build()
    );

    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", new Interval("2012-01-01/2012-01-09"))),
        merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge3()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P8D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("3").size(8).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("3").size(8).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-09/P1D")).version("3").size(8).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-10/P1D")).version("3").size(8).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of("foo", new Interval("2012-01-01/2012-01-04")),
            Pair.of("foo", new Interval("2012-01-04/2012-01-11"))
        ),
        merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge4()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("1").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of("foo", new Interval("2012-01-01/2012-01-03")),
            Pair.of("foo", new Interval("2012-01-03/2012-01-05")),
            Pair.of("foo", new Interval("2012-01-05/2012-01-07"))
        ),
        merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge5()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(15).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("4").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of("foo", new Interval("2012-01-01/2012-01-04")),
            Pair.of("foo", new Interval("2012-01-04/2012-01-07"))
        ),
        merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge6()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("1").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of("foo", new Interval("2012-01-01/2012-01-03")),
            Pair.of("foo", new Interval("2012-01-03/2012-01-07"))
        ),
        merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge7()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("4").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(97).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of("foo", new Interval("2012-01-01/2012-01-03")),
            Pair.of("foo", new Interval("2012-01-03/2012-01-07"))
        ),
        merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge8()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("1").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of("foo", new Interval("2012-01-01/2012-01-04")),
            Pair.of("foo", new Interval("2012-01-04/2012-01-06"))
        ),
        merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge9()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("4").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of("foo", new Interval("2012-01-01/2012-01-04")),
            Pair.of("foo", new Interval("2012-01-04/2012-01-07"))
        ),
        merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge10()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(120).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("4").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", new Interval("2012-01-01/2012-01-03"))),
        merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge11()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(120).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("1").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of("foo", new Interval("2012-01-01/2012-01-03")),
            Pair.of("foo", new Interval("2012-01-03/2012-01-05"))
        ),
        merge(segments)
    );
  }

  @Test
  public void testMergeLinearShardSpecs()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-01/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(1))
                   .size(30)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-02/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(7))
                   .size(40)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-03/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(1500))
                   .size(30)
                   .build()
    );

    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", new Interval("2012-01-01/2012-01-04"))),
        merge(segments)
    );
  }

  @Test
  public void testMergeIncompleteNumberedShardSpecs()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-01/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(0, 2))
                   .size(30)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-02/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(1, 2))
                   .size(40)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-03/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(2, 1500))
                   .size(30)
                   .build()
    );

    Assert.assertEquals(
        ImmutableList.of(),
        merge(segments)
    );
  }

  @Test
  public void testMergeNumberedShardSpecs()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-01/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(0, 2))
                   .size(30)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-01/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(1, 2))
                   .size(30)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-02/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(0, 2))
                   .size(20)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-02/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(1, 2))
                   .size(20)
                   .build()
    );

    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", new Interval("2012-01-01/2012-01-03"))),
        merge(segments)
    );
  }

  @Test
  public void testMergeMixedShardSpecs()
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-01/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(0, 2))
                   .size(25)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-01/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(1, 2))
                   .size(25)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-02/P1D"))
                   .version("1")
                   .size(50)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-03/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(1500))
                   .size(100)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-04/P1D"))
                   .version("1")
                   .shardSpec(new HashBasedNumberedShardSpec(0, 2, mapper))
                   .size(25)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-04/P1D"))
                   .version("1")
                   .shardSpec(new HashBasedNumberedShardSpec(1, 2, mapper))
                   .size(25)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-05/P1D"))
                   .version("1")
                   .shardSpec(new SingleDimensionShardSpec("dim", null, "a", 0))
                   .size(25)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-05/P1D"))
                   .version("1")
                   .shardSpec(new SingleDimensionShardSpec("dim", "a", null, 1))
                   .size(25)
                   .build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of("foo", new Interval("2012-01-01/2012-01-03")),
            Pair.of("foo", new Interval("2012-01-04/2012-01-06"))
        ),
        merge(segments)
    );
  }

  /**
   * Segment timeline
   *
   * Day:    1   2   3   4   5   6   7   8   9   10  11  12  13  14
   * Shard0: |_O_|_O_|_O_|_O_|_S_|_S_|_S_|_O_|_S_|_O_|_S_|_O_|_S_|
   * Shard1:         |_S_|                   |_S_|   |_S_|_S_|
   * Shard2:         |_S_|                   |_S_|   |_S_|
   */
  @Test
  public void testTimeline()
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(120).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(120).build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-03/P1D"))
                   .version("3")
                   .shardSpec(new NumberedShardSpec(0, 3))
                   .size(500)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-03/P1D"))
                   .version("3")
                   .shardSpec(new NumberedShardSpec(1, 3))
                   .size(20)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-03/P1D"))
                   .version("3")
                   .shardSpec(new NumberedShardSpec(2, 3))
                   .size(20)
                   .build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P5D")).version("1").size(500).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(50).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("3").size(50).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-07/P1D")).version("2").size(80).build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-09/P1D"))
                   .version("2")
                   .shardSpec(new SingleDimensionShardSpec("dim", null, "a", 0))
                   .size(10)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-09/P1D"))
                   .version("2")
                   .shardSpec(new SingleDimensionShardSpec("dim", "a", "b", 1))
                   .size(10)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-09/P1D"))
                   .version("2")
                   .shardSpec(new SingleDimensionShardSpec("dim", "b", null, 2))
                   .size(10)
                   .build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-10/P1D")).version("2").size(100).build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-11/P1D"))
                   .version("2")
                   .shardSpec(new HashBasedNumberedShardSpec(0, 3, mapper))
                   .size(50)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-11/P1D"))
                   .version("2")
                   .shardSpec(new HashBasedNumberedShardSpec(1, 3, mapper))
                   .size(50)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-11/P1D"))
                   .version("2")
                   .shardSpec(new HashBasedNumberedShardSpec(2, 3, mapper))
                   .size(50)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-12/P1D"))
                   .version("2")
                   .shardSpec(new NumberedShardSpec(0, 2))
                   .size(100)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-12/P1D"))
                   .version("2")
                   .shardSpec(new NumberedShardSpec(1, 2))
                   .size(1)
                   .build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-13/P1D")).version("2").size(50).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of("foo", new Interval("2012-01-03/2012-01-04")),
            Pair.of("foo", new Interval("2012-01-05/2012-01-07")),
            Pair.of("foo", new Interval("2012-01-07/2012-01-09")),
            Pair.of("foo", new Interval("2012-01-09/2012-01-11")),
            Pair.of("foo", new Interval("2012-01-11/2012-01-12")),
            Pair.of("foo", new Interval("2012-01-12/2012-01-13"))
        ),
        merge(segments)
    );
  }


  /**
   * Runs DruidCoordinatorHadoopSegmentMerger on a particular set of segments and returns the list of unbalanced
   * sections that should be reindexed.
   */
  private static List<Pair<String, Interval>> merge(final Collection<DataSegment> segments)
  {
    final List<Pair<String, Interval>> retVal = Lists.newArrayList();
    final IndexingServiceClient indexingServiceClient = new IndexingServiceClient(null, null, null)
    {
      @Override
      public void hadoopMergeSegments(String dataSource, Interval intervalToReindex)
      {
        retVal.add(Pair.of(dataSource, intervalToReindex));
      }
    };

    final AtomicReference<DatasourceWhitelist> whitelistRef = new AtomicReference<DatasourceWhitelist>(null);
    final DruidCoordinatorHadoopSegmentMerger merger = new DruidCoordinatorHadoopSegmentMerger(
        indexingServiceClient,
        whitelistRef
    );
    final DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams.newBuilder()
                                                                              .withAvailableSegments(
                                                                                  ImmutableSet.copyOf(
                                                                                      segments
                                                                                  )
                                                                              )
                                                                              .withDynamicConfigs(
                                                                                  new CoordinatorDynamicConfig.Builder()
                                                                                      .withMergeBytesLimit(
                                                                                          mergeBytesLimit
                                                                                      )
                                                                                      .build()
                                                                              )
                                                                              .build();
    merger.run(params);
    return retVal;
  }
}
