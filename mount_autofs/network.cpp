#include <stdint.h>
#include <cstring>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>

#include <boost/scope_exit.hpp>

#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "logging.h"
#include "network.h"

#pragma pack(push)
#pragma pack(1)
struct PacketHeader {
    uint16_t mtype;
    uint32_t pbuf_len;
    uint32_t data_len;
};
#pragma pack(pop)

int connect(const char *endpoint) {
    struct addrinfo *servinfo;
    if (strchr(endpoint, ':') != NULL) {
        // Parse TCP address
        std::string hostname(endpoint, strchr(endpoint, ':')-endpoint);
        std::string port(strchr(endpoint, ':')+1);

        struct addrinfo hints;
        memset(&hints, 0, sizeof(struct addrinfo));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;

        int status = getaddrinfo(hostname.c_str(), port.c_str(), &hints, &servinfo);
        if (status < 0) {
            fprintf(stderr, "connect:getaddrinfo: %s\n", gai_strerror(status));
            exit(1);
        }

        // Print addresses
        if (servinfo->ai_family == AF_INET6) {
            fprintf(stderr, "WARNING connect: IPv6 address\n");
        }
    }
    else {
        // Unix socket
        servinfo = (struct addrinfo *)calloc(1, sizeof(struct addrinfo));
        struct sockaddr_un *sa = (struct sockaddr_un *)calloc(1, sizeof(struct sockaddr_un));

        servinfo->ai_addr = (struct sockaddr *)sa;
        servinfo->ai_family = AF_UNIX;
        servinfo->ai_socktype = SOCK_STREAM;
        servinfo->ai_addrlen = sizeof(struct sockaddr_un);
    }

    BOOST_SCOPE_EXIT( (&servinfo) ) {
        freeaddrinfo(servinfo);
    } BOOST_SCOPE_EXIT_END

    // Connect
    int sock = socket(servinfo->ai_family, servinfo->ai_socktype, servinfo->ai_protocol);
    if (sock < 0) {
        perror("connect:socket");
        exit(1);
    }

    int status = ::connect(sock, servinfo->ai_addr, servinfo->ai_addrlen);
    if (status < 0) {
        perror("connect:connect");
        exit(1);
    }

    return sock;
}

int send_packet(int sock, MessageType t, ::google::protobuf::Message *msg, const uint8_t *data, size_t datalen) {
    PacketHeader ph;
    ph.mtype = t;
    ph.pbuf_len = 0;
    ph.data_len = datalen;

    if (datalen > 0) assert(data != NULL);

    // TODO: error checking
    if (msg != NULL) {
        std::string pbuf_bytes;
        msg->SerializeToString(&pbuf_bytes);
        ph.pbuf_len = pbuf_bytes.size();

        send(sock, &ph, sizeof(PacketHeader), 0);
        send(sock, pbuf_bytes.data(), pbuf_bytes.size(), 0);
    }
    else {
        send(sock, &ph, sizeof(PacketHeader), 0);
    }

    if (data != NULL) {
        send(sock, data, datalen, 0);
    }

    return 0;
}

ErrorCode recv_packet(int sock, ::google::protobuf::Message *msg, databuf *dbuf) {
//retry_recv:
    // TODO: error checking
    PacketHeader ph;
    recv(sock, &ph, sizeof(PacketHeader), MSG_WAITALL);

    if (ph.mtype == RESP_ERROR) {
        uint8_t ec;
        recv(sock, &ec, 1, MSG_WAITALL);
        return (ErrorCode)ec;
    }

    if (ph.pbuf_len > 0) {
        assert(msg != NULL);
        databuf msg_buf(ph.pbuf_len);
        recv(sock, &msg_buf[0], ph.pbuf_len, MSG_WAITALL);
        if (!msg->ParseFromArray(&msg_buf[0], ph.pbuf_len))
            return ERR_UNKNOWN;
    }

    if (ph.data_len > 0) {
        dbuf->resize(ph.data_len);
        recv(sock, &dbuf->at(0), ph.data_len, MSG_WAITALL);
    }

    return ERR_NONE;
}
