#include <stdint.h>
#include <cstring>

#include <vector>

#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <zmq.h>

#include "logging.h"
#include "network.h"

void *context = NULL;
void *socket = NULL;

void disconnect() {
    zmq_close(socket);
    zmq_term(context);
}

void connect(const char *endpoint) {
    context = zmq_init(1);
    if (!context) {
        DBZMQERR("zmq_init");
        return;
    }
    socket = zmq_socket(context, ZMQ_REQ);
    if (!socket) {
        DBZMQERR("zmq_socket");
        return;
    }
    if (zmq_connect(socket, endpoint) < 0) {
        DBZMQERR("zmq_connect");
        return;
    }
    atexit(disconnect);
}

int send_packet(MessageType t, ::google::protobuf::Message *msg) {
    zmq_msg_t request;
    zmq_msg_init_size(&request, msg->ByteSize() + 1);
    uint8_t *buf = (uint8_t*)zmq_msg_data(&request);
    buf[0] = (uint8_t)t;
    if (!msg->SerializeToArray(buf+1, zmq_msg_size(&request)-1))
        return false;
    if (zmq_send(socket, &request, 0) < 0) {
        DBZMQERR("zmq_send");
        return -1;
    }
    zmq_msg_close(&request);

    return 0;
}

ErrorCode recv_packet(::google::protobuf::Message *msg) {
    zmq_msg_t reply;
    zmq_msg_init(&reply);
retry_recv:
    if (zmq_recv(socket, &reply, 0) < 0) {
        DBZMQERR("zmq_recv");
        return ERR_UNKNOWN;
    }

    uint8_t *buf = (uint8_t*)zmq_msg_data(&reply);
    if (buf[0] == RESP_ERROR)
        return (ErrorCode)buf[1];

    if (!msg->ParseFromArray(buf+1, zmq_msg_size(&reply)-1))
        return ERR_UNKNOWN;
    zmq_msg_close(&reply);
    return ERR_NONE;
}
