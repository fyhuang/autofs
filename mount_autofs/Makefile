CFLAGS=`pkg-config --cflags fuse` \
	   `pkg-config --cflags protobuf` \
	   `pkg-config --cflags libzmq` \
	   -g -DDEBUG -Wall
CXXFLAGS=$(CFLAGS)
LDFLAGS=`pkg-config --libs fuse` \
		`pkg-config --libs protobuf` \
		`pkg-config --libs libzmq` \
		-g

CC=g++

TARGET=mount_autofs
SOURCES=$(wildcard *.cpp)
PROTO_SOURCES=$(wildcard proto/*.cc)

all: $(TARGET)
$(TARGET): $(SOURCES:.cpp=.o) $(PROTO_SOURCES:.cc=.o)

clean:
	rm *.o
	rm $(TARGET)
