/**
   Specific Data types provide type-safe mapping between Avro schemas and D types.

   This module provides utilities for:
   - Converting between Avro types and D types
   - Generating D type definitions from Avro schemas
   - Type-safe serialization and deserialization

   See_Also: https://avro.apache.org/docs/current/spec.html
*/
module avro.specific;

import std.conv : to;
import std.traits : isNumeric, isSomeString;
import std.array : appender;

import avro.schema : Schema;
import avro.type : Type;
import avro.exception : AvroRuntimeException;

@safe:

/**
   Maps D types to Avro schema types.

   This template provides compile-time type mapping for common D types.
*/
public template AvroType(T) {
  static if (is(T == bool)) {
    enum Type value = Type.BOOLEAN;
  } else static if (is(T == int)) {
    enum Type value = Type.INT;
  } else static if (is(T == long)) {
    enum Type value = Type.LONG;
  } else static if (is(T == float)) {
    enum Type value = Type.FLOAT;
  } else static if (is(T == double)) {
    enum Type value = Type.DOUBLE;
  } else static if (is(T == string)) {
    enum Type value = Type.STRING;
  } else static if (is(T == ubyte[])) {
    enum Type value = Type.BYTES;
  } else {
    static assert(false, "No Avro type mapping for " ~ T.stringof);
  }
}

///
unittest {
  static assert(AvroType!bool.value == Type.BOOLEAN);
  static assert(AvroType!int.value == Type.INT);
  static assert(AvroType!long.value == Type.LONG);
  static assert(AvroType!string.value == Type.STRING);
}

/**
   Base class for specific record types.

   Specific records are D structs/classes that map directly to Avro record schemas.
   They provide type-safe access to Avro data.

   Example:
   ---
   class User : SpecificRecord {
     string name;
     int age;

     override Schema getSchema() { return UserSchema; }
   }
   ---
*/
public abstract class SpecificRecord {
  /// Returns the Avro schema for this record type.
  public abstract Schema getSchema();
}

/**
   Generates D code for a record struct from an Avro schema.

   This is useful for code generation tools that want to create
   type-safe D classes from Avro schemas.
*/
public class SchemaToDGenerator {
  /// Converts an Avro type to a D type string.
  public static string typeToD(Schema schema) {
    switch (schema.getType()) {
      case Type.NULL:
        return "void";
      case Type.BOOLEAN:
        return "bool";
      case Type.INT:
        return "int";
      case Type.LONG:
        return "long";
      case Type.FLOAT:
        return "float";
      case Type.DOUBLE:
        return "double";
      case Type.STRING:
        return "string";
      case Type.BYTES:
        return "ubyte[]";
      case Type.ARRAY:
        return "[]";
      case Type.MAP:
        return "[string]";
      case Type.UNION:
        return "Variant";
      case Type.ENUM:
      case Type.RECORD:
      case Type.FIXED:
        return schema.getName();
      default:
        return "Variant";
    }
  }
}

///
unittest {
  import avro.schema : Schema;

  auto intSchema = Schema.createPrimitive(Type.INT);
  assert(SchemaToDGenerator.typeToD(intSchema) == "int");

  auto stringSchema = Schema.createPrimitive(Type.STRING);
  assert(SchemaToDGenerator.typeToD(stringSchema) == "string");
}

/**
   Helper functions for working with specific types.
*/
public struct SpecificHelper {
  /// Checks if a D type is compatible with an Avro schema.
  public static bool isCompatible(T)(Schema schema) {
    static if (is(T == bool)) {
      return schema.getType() == Type.BOOLEAN;
    } else static if (is(T == int)) {
      return schema.getType() == Type.INT;
    } else static if (is(T == long)) {
      return schema.getType() == Type.LONG;
    } else static if (is(T == float)) {
      return schema.getType() == Type.FLOAT;
    } else static if (is(T == double)) {
      return schema.getType() == Type.DOUBLE;
    } else static if (is(T == string)) {
      return schema.getType() == Type.STRING;
    } else static if (is(T == ubyte[])) {
      return schema.getType() == Type.BYTES;
    } else {
      return false;
    }
  }
}

///
unittest {
  import avro.schema : Schema;

  auto intSchema = Schema.createPrimitive(Type.INT);
  auto stringSchema = Schema.createPrimitive(Type.STRING);

  assert(SpecificHelper.isCompatible!int(intSchema));
  assert(!SpecificHelper.isCompatible!int(stringSchema));
  assert(SpecificHelper.isCompatible!string(stringSchema));
}
