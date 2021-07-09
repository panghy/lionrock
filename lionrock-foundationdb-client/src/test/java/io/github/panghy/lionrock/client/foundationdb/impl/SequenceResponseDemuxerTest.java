package io.github.panghy.lionrock.client.foundationdb.impl;

import com.google.common.util.concurrent.MoreExecutors;
import io.github.panghy.lionrock.proto.GetRangeResponse;
import io.github.panghy.lionrock.proto.GetValueResponse;
import io.github.panghy.lionrock.proto.StreamingDatabaseResponse;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class SequenceResponseDemuxerTest {

  @Test
  void accept_getValue() {
    SequenceResponseDemuxer demuxer = new SequenceResponseDemuxer(MoreExecutors.directExecutor());

    StreamingDatabaseResponseVisitor visitor = mock(StreamingDatabaseResponseVisitor.class);
    demuxer.addHandler(123, visitor);
    StreamingDatabaseResponseVisitor visitor2 = mock(StreamingDatabaseResponseVisitor.class);
    demuxer.addHandler(123, visitor2);

    demuxer.accept(StreamingDatabaseResponse.newBuilder().
        setGetValue(GetValueResponse.newBuilder().setSequenceId(123).build()).
        build());
    // sequence id is respected.
    demuxer.accept(StreamingDatabaseResponse.newBuilder().
        setGetValue(GetValueResponse.newBuilder().setSequenceId(1).build()).
        build());
    // second call is ignored.
    demuxer.accept(StreamingDatabaseResponse.newBuilder().
        setGetValue(GetValueResponse.newBuilder().setSequenceId(123).build()).
        build());

    verify(visitor, times(1)).handleGetValue(any());
    verify(visitor2, times(1)).handleGetValue(any());
  }

  @Test
  void accept_getRange() {
    SequenceResponseDemuxer demuxer = new SequenceResponseDemuxer(MoreExecutors.directExecutor());

    StreamingDatabaseResponseVisitor visitor = mock(StreamingDatabaseResponseVisitor.class);
    demuxer.addHandler(123, visitor);
    StreamingDatabaseResponseVisitor visitor2 = mock(StreamingDatabaseResponseVisitor.class);
    demuxer.addHandler(123, visitor2);

    demuxer.accept(StreamingDatabaseResponse.newBuilder().
        setGetRange(GetRangeResponse.newBuilder().setSequenceId(123).build()).
        build());
    // sequence id is respected.
    demuxer.accept(StreamingDatabaseResponse.newBuilder().
        setGetRange(GetRangeResponse.newBuilder().setSequenceId(1).build()).
        build());
    // second call is not ignored.
    demuxer.accept(StreamingDatabaseResponse.newBuilder().
        setGetRange(GetRangeResponse.newBuilder().setSequenceId(123).build()).
        build());
    // third call is not ignored.
    demuxer.accept(StreamingDatabaseResponse.newBuilder().
        setGetRange(GetRangeResponse.newBuilder().setSequenceId(123).setDone(true).build()).
        build());
    // fourth call is ignored.
    demuxer.accept(StreamingDatabaseResponse.newBuilder().
        setGetRange(GetRangeResponse.newBuilder().setSequenceId(123).setDone(true).build()).
        build());

    verify(visitor, times(3)).handleGetRange(any());
    verify(visitor2, times(3)).handleGetRange(any());
  }
}