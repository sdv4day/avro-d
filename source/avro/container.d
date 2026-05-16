/**
   Object Container Files provide a standard way to store Avro data in files.

   An Avro Object Container File consists of:
   - A magic number "Obj\x01"
   - Metadata map (containing schema and codec)
   - Sync marker (16 bytes)
   - One or more data blocks

   See_Also: https://avro.apache.org/docs/current/spec.html#Object+Container+Files
*/
module avro.container;

import std.conv : to;
import std.array : appender;
import std.zlib;
import std.random : uniform;

import avro.schema : Schema;
import avro.type : Type;
import avro.codec.binaryencoder : BinaryEncoder, binaryEncoder;
import avro.codec.binarydecoder : BinaryDecoder, binaryDecoder;
import avro.exception : AvroRuntimeException;

@safe:

/// Magic bytes that identify an Avro container file.
public enum ubyte[4] MAGIC = [cast(ubyte)'O', cast(ubyte)'b', cast(ubyte)'j', cast(ubyte)0x01];

/// Default sync marker size in bytes.
public enum size_t SYNC_SIZE = 16;

/// Supported compression codecs for container files.
public enum Codec {
  NULL,     /// No compression
  DEFLATE,  /// Deflate compression (zlib)
  SNAPPY,   /// Snappy compression (not yet implemented)
  ZSTD      /// Zstandard compression (not yet implemented)
}

/**
   Header of an Avro container file.

   The header contains:
   - Magic bytes
   - Schema (as JSON)
   - Compression codec
   - Sync marker
*/
public struct ContainerHeader {
  ubyte[4] magic;
  Schema schema;
  Codec codec;
  ubyte[SYNC_SIZE] syncMarker;

  /// Validates that the magic bytes are correct.
  public bool validateMagic() const {
    return magic == MAGIC;
  }
}

/**
   A block of data in an Avro container file.

   Each block contains:
   - Count of objects in the block
   - Size of the block data in bytes
   - The serialized objects
   - The sync marker
*/
public struct DataBlock {
  long objectCount;
  long blockSize;
  ubyte[] data;
  ubyte[SYNC_SIZE] syncMarker;
}

/**
   Writer for Avro Object Container Files.

   Example:
   ---
   import avro.container;
   import avro.schema;

   auto schema = parser.parseText(`{"type": "record", "name": "Test", "fields": [{"name": "id", "type": "int"}]}`);
   auto writer = containerWriter(schema);
   auto header = writer.writeHeader();
   // Write data blocks...
   ---
*/
public class ContainerWriter {
  private Schema schema;
  private Codec codec;
  private ubyte[SYNC_SIZE] syncMarker;

  /// Creates a new container writer.
  this(Schema schema, Codec codec = Codec.NULL) {
    this.schema = schema;
    this.codec = codec;
    generateSyncMarker();
  }

  /// Generates a random sync marker.
  private void generateSyncMarker() @trusted {
    foreach (size_t i; 0 .. SYNC_SIZE) {
      syncMarker[i] = cast(ubyte)uniform(0, 256);
    }
  }

  /// Writes the file header to the output.
  public ubyte[] writeHeader() {
    auto output = appender!(ubyte[])();

    output.put(MAGIC[]);

    auto encoder = binaryEncoder(output);

    encoder.writeMapStart();
    encoder.setItemCount(2);

    encoder.startItem();
    encoder.writeMapKey("avro.schema");
    encoder.writeString(schema.toString());

    encoder.startItem();
    encoder.writeMapKey("avro.codec");
    encoder.writeString(codecToString(codec));

    encoder.writeMapEnd();

    encoder.writeFixed(syncMarker[]);
    encoder.flush();

    return output[];
  }

  /// Writes a data block to the output.
  public ubyte[] writeBlock(DataBlock block) {
    auto output = appender!(ubyte[])();
    auto encoder = binaryEncoder(output);

    encoder.writeLong(block.objectCount);
    encoder.writeLong(block.blockSize);

    encoder.flush();

    ubyte[] dataToWrite;
    if (codec == Codec.DEFLATE) {
      dataToWrite = compressBlock(block.data);
    } else {
      dataToWrite = block.data;
    }

    output.put(dataToWrite);

    encoder = binaryEncoder(output);
    encoder.writeFixed(syncMarker[]);
    encoder.flush();

    return output[];
  }

  /// Compresses a block using the configured codec.
  private ubyte[] compressBlock(ubyte[] data) @trusted {
    if (codec == Codec.DEFLATE) {
      return cast(ubyte[])std.zlib.compress(data);
    }
    return data;
  }

  /// Converts a codec enum to its string representation.
  private static string codecToString(Codec c) {
    final switch (c) {
      case Codec.NULL:
        return "null";
      case Codec.DEFLATE:
        return "deflate";
      case Codec.SNAPPY:
        return "snappy";
      case Codec.ZSTD:
        return "zstd";
    }
  }

  /// Returns the sync marker.
  public ubyte[SYNC_SIZE] getSyncMarker() const {
    return syncMarker;
  }
}

///
unittest {
  import avro.schema : Schema;

  auto schema = Schema.createPrimitive(Type.INT);
  auto writer = new ContainerWriter(schema);

  auto header = writer.writeHeader();
  assert(header.length > 4);
  assert(header[0 .. 4] == MAGIC);
}

/**
   Reader for Avro Container Files.

   Example:
   ---
   import avro.container;

   auto reader = containerReader(fileData);
   auto header = reader.readHeader();
   while (reader.hasNext()) {
     auto block = reader.readBlock();
     // Process block...
   }
   ---
*/
public class ContainerReader {
  private ubyte[] fileData;
  private size_t position;
  private ContainerHeader header;
  private bool headerRead = false;

  /// Creates a container reader from the given data.
  this(ubyte[] data) {
    this.fileData = data;
    this.position = 0;
  }

  /// Reads and validates the file header.
  public ContainerHeader readHeader() @trusted {
    if (headerRead) {
      return header;
    }

    if (fileData.length < 4) {
      throw new AvroRuntimeException("File too small to be an Avro container file");
    }

    header.magic[] = fileData[position .. position + 4];
    position += 4;

    if (!header.validateMagic()) {
      throw new AvroRuntimeException("Invalid Avro container file: bad magic bytes");
    }

    auto decoder = binaryDecoder(fileData[position .. $]);

    long mapCount = decoder.readLong();
    string schemaJson;
    string codecName = "null";

    for (long i = 0; i < mapCount; i++) {
      string key = decoder.readString();
      auto valueBytes = decoder.readBytes();

      if (key == "avro.schema") {
        schemaJson = cast(string)valueBytes;
      } else if (key == "avro.codec") {
        codecName = cast(string)valueBytes;
      }
    }

    decoder.readLong();

    position += cast(size_t)(fileData.length - decoder.remainingData().length);

    header.codec = codecFromString(codecName);

    if (schemaJson !is null) {
      import avro.parser : Parser;
      auto parser = new Parser();
      header.schema = parser.parseText(schemaJson);
    }

    header.syncMarker[] = decoder.readFixed(SYNC_SIZE);
    position += SYNC_SIZE;

    headerRead = true;
    return header;
  }

  /// Checks if there are more blocks to read.
  public bool hasNext() {
    return position < fileData.length;
  }

  /// Reads the next data block.
  public DataBlock readBlock() {
    DataBlock block;

    auto decoder = binaryDecoder(fileData[position .. $]);

    block.objectCount = decoder.readLong();
    block.blockSize = decoder.readLong();

    position += cast(size_t)(fileData.length - position - decoder.remainingData().length);

    block.data = fileData[position .. position + cast(size_t)block.blockSize];
    position += cast(size_t)block.blockSize;

    decoder = binaryDecoder(fileData[position .. $]);
    block.syncMarker[] = decoder.readFixed(SYNC_SIZE);
    position += SYNC_SIZE;

    return block;
  }

  /// Returns the schema from the header.
  public Schema getSchema() {
    if (!headerRead) {
      readHeader();
    }
    return header.schema;
  }

  /// Returns the codec from the header.
  public Codec getCodec() {
    return header.codec;
  }

  /// Returns the current position in the file.
  public size_t getPosition() const {
    return position;
  }

  /// Converts a codec string to enum.
  private static Codec codecFromString(string name) {
    switch (name) {
      case "null":
        return Codec.NULL;
      case "deflate":
        return Codec.DEFLATE;
      case "snappy":
        return Codec.SNAPPY;
      case "zstd":
        return Codec.ZSTD;
      default:
        return Codec.NULL;
    }
  }
}

///
unittest {
  ubyte[] data = [cast(ubyte)'O', 'b', 'j', 0x01];
  auto reader = new ContainerReader(data);
  assert(reader.hasNext());
}

/**
   Convenience function to create a ContainerWriter.
*/
public auto containerWriter(Schema schema, Codec codec = Codec.NULL) {
  return new ContainerWriter(schema, codec);
}

/**
   Convenience function to create a ContainerReader.
*/
public auto containerReader(ubyte[] data) {
  return new ContainerReader(data);
}
