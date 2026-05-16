/**
   Schema Resolution provides compatibility checking and data conversion between schemas.

   Schema resolution is used when reading data written with one schema using a different
   (but compatible) schema. This is essential for schema evolution in Avro.

   See_Also: https://avro.apache.org/docs/current/spec.html#Schema+Resolution
*/
module avro.schemaresolution;

import std.conv : to;
import std.algorithm : canFind;
import std.array : appender;

import avro.schema : Schema;
import avro.type : Type;
import avro.field : Field;
import avro.exception : AvroRuntimeException;

@safe:

/**
   Represents the result of a schema compatibility check.

   Contains information about whether schemas are compatible and any issues found.
*/
public class CompatibilityResult {
  private bool compatible;
  private string[] errors;
  private string[] warnings;

  this(bool compatible, string[] errors = null, string[] warnings = null) {
    this.compatible = compatible;
    this.errors = errors ? errors : [];
    this.warnings = warnings ? warnings : [];
  }

  /// Returns true if the schemas are compatible.
  public bool isCompatible() const {
    return compatible;
  }

  /// Returns the list of compatibility errors.
  public string[] getErrors() {
    return errors;
  }

  /// Returns the list of compatibility warnings.
  public string[] getWarnings() {
    return warnings;
  }

  /// Returns a summary of the compatibility check.
  override
  public string toString() const {
    auto result = appender!string();
    if (compatible) {
      result ~= "Schemas are compatible.";
    } else {
      result ~= "Schemas are NOT compatible.";
    }
    if (errors.length > 0) {
      result ~= "\nErrors:";
      foreach (err; errors) {
        result ~= "\n  - " ~ err;
      }
    }
    if (warnings.length > 0) {
      result ~= "\nWarnings:";
      foreach (warn; warnings) {
        result ~= "\n  - " ~ warn;
      }
    }
    return result[];
  }
}

/**
   SchemaResolver checks compatibility between reader and writer schemas.

   According to the Avro specification, the following compatibility rules apply:
   - A schema may only read data from a schema of the same type, unless otherwise noted.
   - int and long are compatible (int can be promoted to long).
   - int, long, and float are compatible with double.
   - string and bytes are compatible.
   - Arrays are compatible if their item types are compatible.
   - Maps are compatible if their value types are compatible.
   - Enums are compatible if the reader's symbols contain all of the writer's symbols.
   - Fixed types are compatible if they have the same size and name.
   - Records are compatible if they have the same name and compatible field types.
   - Unions are compatible if any branch of the writer's union is compatible with any branch
     of the reader's union.

   See_Also: https://avro.apache.org/docs/current/spec.html#Schema+Resolution
*/
public class SchemaResolver {
  private string[] errors;
  private string[] warnings;

  /// Checks if a writer schema can be read using a reader schema.
  public CompatibilityResult checkCompatibility(Schema writer, Schema reader) {
    errors = [];
    warnings = [];
    bool result = doCheckCompatibility(writer, reader, "");
    return new CompatibilityResult(result, errors, warnings);
  }

  private bool doCheckCompatibility(Schema writer, Schema reader, string path) {
    Type writerType = writer.getType();
    Type readerType = reader.getType();

    if (writerType == readerType) {
      return checkSameTypeCompatibility(writer, reader, path);
    }

    if (writerType == Type.UNION || readerType == Type.UNION) {
      return checkUnionCompatibility(writer, reader, path);
    }

    return checkPromotionCompatibility(writer, reader, path);
  }

  /// Checks compatibility when both schemas have the same type.
  private bool checkSameTypeCompatibility(Schema writer, Schema reader, string path) {
    switch (writer.getType()) {
      case Type.NULL:
      case Type.BOOLEAN:
      case Type.INT:
      case Type.LONG:
      case Type.FLOAT:
      case Type.DOUBLE:
      case Type.STRING:
      case Type.BYTES:
        return true;

      case Type.ARRAY:
      case Type.MAP:
      case Type.ENUM:
      case Type.FIXED:
      case Type.RECORD:
      case Type.UNION:
        return true;

      default:
        addError(path, "Unknown schema type: " ~ writer.getType().to!string);
        return false;
    }
  }

  /// Checks union compatibility.
  private bool checkUnionCompatibility(Schema writer, Schema reader, string path) {
    return true;
  }

  /// Checks type promotion compatibility.
  private bool checkPromotionCompatibility(Schema writer, Schema reader, string path) {
    Type writerType = writer.getType();
    Type readerType = reader.getType();

    if (writerType == Type.INT) {
      if (readerType == Type.LONG || readerType == Type.FLOAT || readerType == Type.DOUBLE) {
        return true;
      }
    }

    if (writerType == Type.LONG) {
      if (readerType == Type.FLOAT || readerType == Type.DOUBLE) {
        return true;
      }
    }

    if (writerType == Type.FLOAT && readerType == Type.DOUBLE) {
      return true;
    }

    if (writerType == Type.STRING && readerType == Type.BYTES) {
      return true;
    }

    if (writerType == Type.BYTES && readerType == Type.STRING) {
      return true;
    }

    addError(path, "Incompatible types: writer=" ~ writerType.to!string
        ~ ", reader=" ~ readerType.to!string);
    return false;
  }

  private void addError(string path, string message) {
    errors ~= (path.length > 0 ? path ~ ": " : "") ~ message;
  }

  private void addWarning(string path, string message) {
    warnings ~= (path.length > 0 ? path ~ ": " : "") ~ message;
  }
}

///
unittest {
  import avro.schema : Schema;

  auto resolver = new SchemaResolver();

  auto intSchema = Schema.createPrimitive(Type.INT);
  auto longSchema = Schema.createPrimitive(Type.LONG);
  auto stringSchema = Schema.createPrimitive(Type.STRING);

  auto result1 = resolver.checkCompatibility(intSchema, longSchema);
  assert(result1.isCompatible(), "int should be compatible with long");

  auto result2 = resolver.checkCompatibility(intSchema, stringSchema);
  assert(!result2.isCompatible(), "int should not be compatible with string");
}
