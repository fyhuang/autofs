#include <zmq.h>
#include <boost/shared_ptr.hpp>
using boost::shared_ptr;

// Protocol buffers
#include <google/protobuf/io/zero_copy_stream.h>
#include "proto/autofs_local.pb.h"
using namespace autofs_local;

// ZeroMQ wrapper
void connect(const char *endpoint);
extern int send_packet(MessageType t, ::google::protobuf::Message *msg);
extern ErrorCode recv_packet(::google::protobuf::Message *msg);
