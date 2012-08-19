#include <vector>

#include <boost/shared_ptr.hpp>
using boost::shared_ptr;

// Protocol buffers
#include <google/protobuf/io/zero_copy_stream.h>
#include "protobuf/autofs_local.pb.h"
using namespace autofs_local;

int connect(const char *endpoint);

typedef std::vector<uint8_t> databuf;
int send_packet(int sock, MessageType t, ::google::protobuf::Message *msg, const uint8_t *data = NULL, size_t datalen = 0);
ErrorCode recv_packet(int sock, ::google::protobuf::Message *msg, databuf *dbuf = NULL);
