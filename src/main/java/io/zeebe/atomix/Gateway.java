package io.zeebe.atomix;

import io.atomix.core.Atomix;
import io.atomix.primitive.Consistency;
import io.atomix.primitive.Replication;
import io.atomix.primitive.log.LogClient;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.utils.net.Address;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.zeebe.gateway.protocol.GatewayGrpc.GatewayImplBase;
import io.zeebe.gateway.protocol.GatewayOuterClass.PublishMessageRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.PublishMessageResponse;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Gateway extends GatewayImplBase {

  public static final Logger LOG = LoggerFactory.getLogger(Gateway.class);
  private LogClient logClient;
  private final Atomix atomix;
  private final Server server;
  private final DistributedLogProtocol zeebeLogProtocol;

  public Gateway() {
    zeebeLogProtocol = DistributedLogProtocol.builder("zeebe")
        .withConsistency(Consistency.LINEARIZABLE)
        .withReplication(Replication.ASYNCHRONOUS)
        .build();

    atomix = Atomix.builder()
        .withMemberId("client")
        .withAddress(Address.from(10001))
        .withClusterId("test")
        .withMulticastEnabled()
        .build();

    server = ServerBuilder.forPort(26500)
        .addService(this)
        .build();
    }

  public void run() throws IOException {
    atomix.start().join();
    logClient = zeebeLogProtocol.newClient(atomix.getPartitionService());
    server.start();
  }

  public static void main(String[] args) throws IOException {
    new Gateway().run();
  }

  @Override
  public void publishMessage(PublishMessageRequest request,
      StreamObserver<PublishMessageResponse> responseObserver) {
    LOG.info("Received publish message request: {}", request);

    logClient.getPartition(request.getCorrelationKey())
        .producer()
        .append(request.getNameBytes().toByteArray())
        .thenAccept(position -> {
          LOG.info("Request was append on position: {}", position);
          responseObserver.onNext(PublishMessageResponse.getDefaultInstance());
          responseObserver.onCompleted();
        })
        .exceptionally(error -> {
          LOG.error("Failed to append request", error);
          responseObserver.onError(error);
          return null;
        });
  }
}
