all:
	mkdir -p ../autofs/protobuf
	mkdir -p ../mount_autofs/protobuf
	mkdir -p ../tools/protobuf
	touch ../autofs/protobuf/__init__.py
	touch ../tools/protobuf/__init__.py
	
	protoc --python_out=../autofs/protobuf/ *.proto
	protoc --python_out=../tools/protobuf/ *.proto
	protoc --cpp_out=../mount_autofs/protobuf/ *.proto
