package io.zeebe.atomix;

import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.core.Atomix;
import io.atomix.primitive.Consistency;
import io.atomix.primitive.Replication;
import io.atomix.primitive.log.LogClient;
import io.atomix.primitive.log.LogRecord;
import io.atomix.primitive.log.LogSession;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.log.partition.LogPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.net.Address;
import java.io.File;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Broker implements ClusterMembershipEventListener {

  public static final Logger LOG = LoggerFactory.getLogger(Broker.class);

  private final String memberId;
  private final int port;

  public Broker(String memberId, int port) {
    this.memberId = memberId;
    this.port = port;
  }

  private void run() {
    String memberDirPath = "/tmp/atomix/" + memberId;
    RaftPartitionGroup systemPartitionGroup = RaftPartitionGroup.builder("system")
        .withDataDirectory(new File(memberDirPath + "/system"))
        .withNumPartitions(1)
        .withMembers("broker1", "broker2", "broker3")
        .build();

    LogPartitionGroup zeebePartitionGroup = LogPartitionGroup.builder("zeebe")
        .withNumPartitions(3)
        .withDataDirectory(new File(memberDirPath + "/zeebe"))
        .withFlushOnCommit()
        .build();

    DistributedLogProtocol zeebeLogProtocol = DistributedLogProtocol.builder("zeebe")
        .withConsistency(Consistency.LINEARIZABLE)
        .withReplication(Replication.ASYNCHRONOUS)
        .build();

    Atomix atomix =
        Atomix.builder()
            .withMemberId(memberId)
            .withAddress(Address.from(port))
            .withClusterId("test")
            .withMulticastEnabled()
            .withManagementGroup(systemPartitionGroup)
            .withPartitionGroups(zeebePartitionGroup)
            .build();
    atomix.start().join();

    atomix.getMembershipService().addListener(this);

    LogClient logClient = zeebeLogProtocol.newClient(atomix.getPartitionService());

    System.out.println("Partition ids: " + logClient.getPartitionIds());
    logClient.getPartitions()
        .forEach(partition -> {
  //        partition.consumer().consume(recordConsumer(partition));
        });
  }

  public static void main(String[] args) {
    assert args.length == 2;
    new Broker(args[0], Integer.parseInt(args[1])).run();
  }

  @Override
  public void event(ClusterMembershipEvent clusterMembershipEvent) {
    LOG.warn("Cluster change {}", clusterMembershipEvent);
  }

  public Consumer<LogRecord> recordConsumer(LogSession logSession) {
    return logRecord -> LOG.error("Consumed record {} from partition {} with message {}", logRecord, logSession.partitionId(), new String(logRecord.value()));
  }

}
