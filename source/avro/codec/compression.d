/**
   Codec support for Avro data compression.

   Avro supports several compression codecs for data storage:
   - null: No compression
   - deflate: Standard DEFLATE compression (zlib)
   - snappy: Fast compression (requires external library)
   - zstd: Zstandard compression (requires external library)

   See_Also: https://avro.apache.org/docs/current/spec.html#Object+Container+Files
*/
module avro.codec.compression;

import std.conv : to;
import std.zlib;

import avro.exception : AvroRuntimeException;

@safe:

/**
   Base interface for compression codecs.

   Each codec must be able to compress and decompress byte arrays.
*/
public interface Codec {
  /// Returns the name of this codec.
  string getName() const;

  /// Compresses the input data.
  ubyte[] compress(const(ubyte)[] data);

  /// Decompresses the input data.
  ubyte[] decompress(const(ubyte)[] data);
}

/**
   Null codec - no compression.

   This codec simply passes data through unchanged.
*/
public class NullCodec : Codec {
  /// Returns "null".
  override
  public string getName() const {
    return "null";
  }

  /// Returns the input data unchanged.
  override
  public ubyte[] compress(const(ubyte)[] data) @trusted {
    return data.dup;
  }

  /// Returns the input data unchanged.
  override
  public ubyte[] decompress(const(ubyte)[] data) @trusted {
    return data.dup;
  }
}

///
unittest {
  auto codec = new NullCodec();
  assert(codec.getName() == "null");

  ubyte[] data = [1, 2, 3, 4, 5];
  auto compressed = codec.compress(data);
  assert(compressed == data);

  auto decompressed = codec.decompress(data);
  assert(decompressed == data);
}

/**
   Deflate codec using zlib compression.

   This codec uses the standard DEFLATE algorithm with configurable compression level.
*/
public class DeflateCodec : Codec {
  private int level;

  /// Creates a deflate codec with the specified compression level (1-9).
  this(int level = 6) {
    this.level = level;
  }

  /// Returns "deflate".
  override
  public string getName() const {
    return "deflate";
  }

  /// Compresses the data using DEFLATE.
  override
  public ubyte[] compress(const(ubyte)[] data) @trusted {
    if (data.length == 0) {
      return [];
    }
    auto result = std.zlib.compress(cast(ubyte[])data, level);
    return cast(ubyte[])result;
  }

  /// Decompresses the data using DEFLATE.
  override
  public ubyte[] decompress(const(ubyte)[] data) @trusted {
    if (data.length == 0) {
      return [];
    }
    auto result = std.zlib.uncompress(cast(ubyte[])data);
    return cast(ubyte[])result;
  }
}

///
unittest {
  auto codec = new DeflateCodec();
  assert(codec.getName() == "deflate");

  ubyte[] data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  auto compressed = codec.compress(data);
  assert(compressed.length > 0);

  auto decompressed = codec.decompress(compressed);
  assert(decompressed == data);
}

/**
   Factory for creating codec instances by name.

   This provides a centralized way to create codecs based on configuration.
*/
public class CodecFactory {
  /// Creates a codec by name.
  public static Codec createCodec(string name) {
    switch (name) {
      case "null":
        return new NullCodec();
      case "deflate":
        return new DeflateCodec();
      case "snappy":
        throw new AvroRuntimeException("Snappy codec not yet implemented. Use 'deflate' instead.");
      case "zstd":
        throw new AvroRuntimeException("Zstandard codec not yet implemented. Use 'deflate' instead.");
      default:
        throw new AvroRuntimeException("Unknown codec: " ~ name);
    }
  }

  /// Returns a list of available codec names.
  public static string[] getAvailableCodecs() {
    return ["null", "deflate"];
  }

  /// Checks if a codec is available.
  public static bool isCodecAvailable(string name) {
    foreach (string codec; getAvailableCodecs()) {
      if (codec == name) return true;
    }
    return false;
  }
}

///
unittest {
  auto nullCodec = CodecFactory.createCodec("null");
  assert(nullCodec.getName() == "null");

  auto deflateCodec = CodecFactory.createCodec("deflate");
  assert(deflateCodec.getName() == "deflate");

  assert(CodecFactory.isCodecAvailable("null"));
  assert(CodecFactory.isCodecAvailable("deflate"));
  assert(!CodecFactory.isCodecAvailable("snappy"));
}

/**
   Utility class for compressing/decompressing blocks of data.

   This class provides a simple interface for working with compressed data blocks.
*/
public struct CompressedBlock {
  ubyte[] data;
  string codecName;
  size_t uncompressedSize;

  /// Returns true if this block is compressed.
  public bool isCompressed() const {
    return codecName != "null";
  }
}

/**
   Helper functions for block compression.
*/
public struct BlockCompressor {
  /// Compresses a block using the specified codec.
  public static CompressedBlock compress(const(ubyte)[] data, Codec codec) {
    CompressedBlock block;
    block.uncompressedSize = data.length;
    block.codecName = codec.getName();
    block.data = codec.compress(data);
    return block;
  }

  /// Decompresses a block.
  public static ubyte[] decompress(CompressedBlock block, Codec codec) {
    return codec.decompress(block.data);
  }
}

///
unittest {
  auto codec = new DeflateCodec();
  ubyte[] data = [1, 2, 3, 4, 5];

  auto compressed = BlockCompressor.compress(data, codec);
  assert(compressed.codecName == "deflate");
  assert(compressed.isCompressed());

  auto decompressed = BlockCompressor.decompress(compressed, codec);
  assert(decompressed == data);
}
