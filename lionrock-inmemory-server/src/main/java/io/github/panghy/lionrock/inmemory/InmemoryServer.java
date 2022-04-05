package io.github.panghy.lionrock.inmemory;

import io.github.panghy.lionrock.proto.TransactionalKeyValueStoreGrpc;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

@SpringBootApplication
@GRpcService
@Component
public class InmemoryServer extends TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreImplBase {

}
