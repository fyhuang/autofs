all:
	mkdir -p ../autofs/proto
	mkdir -p ../mount_autofs/proto
	mkdir -p ../tools/local_proto
	protoc --python_out=../autofs/proto/ *.proto
	protoc --python_out=../tools/proto/ *.proto
	protoc --cpp_out=../mount_autofs/proto/ *.proto
