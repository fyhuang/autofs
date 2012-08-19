CFLAGS=`pkg-config --cflags fuse` \
	   `pkg-config --cflags protobuf` \
	   -g -DDEBUG -Wall
CXXFLAGS=$(CFLAGS)
LDFLAGS=`pkg-config --libs fuse` \
		`pkg-config --libs protobuf` \
		-g

CC=g++

TARGET=mount_autofs
SOURCES=$(wildcard *.cpp)
PROTO_SOURCES=$(wildcard protobuf/*.cc)

all: $(TARGET)
$(TARGET): $(SOURCES:.cpp=.o) $(PROTO_SOURCES:.cc=.o)

clean:
	rm *.o
	rm $(TARGET)
