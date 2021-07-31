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
    long seqId = demuxer.addHandler(visitor);
    StreamingDatabaseResponseVisitor visitor2 = mock(StreamingDatabaseResponseVisitor.class);
    long seqId2 = demuxer.addHandler(visitor2);

    demuxer.accept(StreamingDatabaseResponse.newBuilder().
        setGetValue(GetValueResponse.newBuilder().setSequenceId(seqId).build()).
        build());
    // throws on unknown id
    try {
      demuxer.accept(StreamingDatabaseResponse.newBuilder().
          setGetValue(GetValueResponse.newBuilder().setSequenceId(1).build()).
          build());
    } catch (IllegalArgumentException expected) {
    }
    // duplicate calls throws.
    try {
      demuxer.accept(StreamingDatabaseResponse.newBuilder().
          setGetValue(GetValueResponse.newBuilder().setSequenceId(seqId).build()).
          build());
    } catch (IllegalArgumentException expected) {
    }
    // accepts second handler.
    demuxer.accept(StreamingDatabaseResponse.newBuilder().
        setGetValue(GetValueResponse.newBuilder().setSequenceId(seqId2).build()).
        build());

    verify(visitor, times(1)).handleGetValue(any());
    verify(visitor2, times(1)).handleGetValue(any());
  }

  @Test
  void accept_getRange() {
    SequenceResponseDemuxer demuxer = new SequenceResponseDemuxer(MoreExecutors.directExecutor());

    StreamingDatabaseResponseVisitor visitor = mock(StreamingDatabaseResponseVisitor.class);
    long seqId = demuxer.addHandler(visitor);

    demuxer.accept(StreamingDatabaseResponse.newBuilder().
        setGetRange(GetRangeResponse.newBuilder().setSequenceId(seqId).build()).
        build());
    // throws on unknown id
    try {
      demuxer.accept(StreamingDatabaseResponse.newBuilder().
          setGetValue(GetValueResponse.newBuilder().setSequenceId(1).build()).
          build());
    } catch (IllegalArgumentException expected) {
    }
    // second call is not ignored.
    demuxer.accept(StreamingDatabaseResponse.newBuilder().
        setGetRange(GetRangeResponse.newBuilder().setSequenceId(seqId).build()).
        build());
    // third call is not ignored.
    demuxer.accept(StreamingDatabaseResponse.newBuilder().
        setGetRange(GetRangeResponse.newBuilder().setSequenceId(seqId).setDone(true).build()).
        build());
    // fourth call is ignored.

    // duplicate calls throws.
    try {
      demuxer.accept(StreamingDatabaseResponse.newBuilder().
          setGetRange(GetRangeResponse.newBuilder().setSequenceId(seqId).setDone(true).build()).
          build());
    } catch (IllegalArgumentException expected) {
    }

    verify(visitor, times(3)).handleGetRange(any());
  }
}