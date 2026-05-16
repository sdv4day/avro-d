/**
   Logical types provide a way to add additional meaning to primitive types.

   Avro logical types are annotations on primitive types that add semantic meaning.
   For example, a string can be annotated as a date, or a long as a timestamp.

   See_Also: https://avro.apache.org/docs/current/spec.html#Logical+Types
*/
module avro.logicaltype;

import std.conv : to;

import avro.type : Type;
import avro.exception : AvroRuntimeException;

@safe:

/**
   Base class for all logical types.

   Logical types are specified using the "logicalType" attribute in a schema.
   They define how to convert between the underlying Avro type and a more meaningful D type.
*/
public abstract class LogicalType {
  protected string name;

  this(string name) {
    this.name = name;
  }

  /// Returns the name of this logical type.
  public string getName() const {
    return name;
  }

  /// Returns the underlying Avro type that this logical type annotates.
  public abstract Type[] getSupportedTypes() const;

  /// Validates that this logical type can be applied to the given schema type.
  public bool validate(Type schemaType) const {
    foreach (Type t; getSupportedTypes()) {
      if (t == schemaType) return true;
    }
    return false;
  }
}

/**
   Represents a decimal number with fixed precision and scale.

   The decimal logical type annotates Avro bytes or fixed types.
   The underlying type stores unscaled integers, which are scaled by 10^(-scale).

   See_Also: https://avro.apache.org/docs/current/spec.html#Decimal
*/
public class DecimalLogicalType : LogicalType {
  private int precision;
  private int scale;

  /// Creates a decimal logical type with the given precision and scale.
  this(int precision, int scale = 0) {
    super("decimal");
    if (precision <= 0) {
      throw new AvroRuntimeException("Precision must be positive: " ~ precision.to!string);
    }
    if (scale < 0) {
      throw new AvroRuntimeException("Scale must be non-negative: " ~ scale.to!string);
    }
    if (scale > precision) {
      throw new AvroRuntimeException("Scale cannot exceed precision: scale=" ~ scale.to!string
          ~ ", precision=" ~ precision.to!string);
    }
    this.precision = precision;
    this.scale = scale;
  }

  /// Returns the precision (total number of digits).
  public int getPrecision() const {
    return precision;
  }

  /// Returns the scale (number of digits after decimal point).
  public int getScale() const {
    return scale;
  }

  override
  public Type[] getSupportedTypes() const {
    return [Type.BYTES, Type.FIXED];
  }
}

///
unittest {
  auto decimal = new DecimalLogicalType(10, 2);
  assert(decimal.getName() == "decimal");
  assert(decimal.getPrecision() == 10);
  assert(decimal.getScale() == 2);
  assert(decimal.validate(Type.BYTES));
  assert(!decimal.validate(Type.INT));
}

/**
   Represents a UUID (Universally Unique Identifier).

   The uuid logical type annotates an Avro string type.
   The string must conform to the standard UUID string representation.

   See_Also: https://avro.apache.org/docs/current/spec.html#UUID
*/
public class UuidLogicalType : LogicalType {
  /// Creates a UUID logical type.
  this() {
    super("uuid");
  }

  override
  public Type[] getSupportedTypes() const {
    return [Type.STRING];
  }
}

///
unittest {
  auto uuid = new UuidLogicalType();
  assert(uuid.getName() == "uuid");
  assert(uuid.validate(Type.STRING));
  assert(!uuid.validate(Type.INT));
}

/**
   Represents a date (without time or timezone).

   The date logical type annotates an Avro int type.
   The int stores the number of days since the Unix epoch (1970-01-01).

   See_Also: https://avro.apache.org/docs/current/spec.html#Date
*/
public class DateLogicalType : LogicalType {
  /// Creates a date logical type.
  this() {
    super("date");
  }

  override
  public Type[] getSupportedTypes() const {
    return [Type.INT];
  }
}

///
unittest {
  auto date = new DateLogicalType();
  assert(date.getName() == "date");
  assert(date.validate(Type.INT));
  assert(!date.validate(Type.STRING));
}

/**
   Represents a time in milliseconds since midnight.

   The time-millis logical type annotates an Avro int type.
   The int stores the number of milliseconds after midnight (00:00:00.000).

   See_Also: https://avro.apache.org/docs/current/spec.html#Time+%28millisecond+precision%29
*/
public class TimeMillisLogicalType : LogicalType {
  /// Creates a time-millis logical type.
  this() {
    super("time-millis");
  }

  override
  public Type[] getSupportedTypes() const {
    return [Type.INT];
  }
}

///
unittest {
  auto time = new TimeMillisLogicalType();
  assert(time.getName() == "time-millis");
  assert(time.validate(Type.INT));
}

/**
   Represents a time in microseconds since midnight.

   The time-micros logical type annotates an Avro long type.
   The long stores the number of microseconds after midnight (00:00:00.000000).

   See_Also: https://avro.apache.org/docs/current/spec.html#Time+%28microsecond+precision%29
*/
public class TimeMicrosLogicalType : LogicalType {
  /// Creates a time-micros logical type.
  this() {
    super("time-micros");
  }

  override
  public Type[] getSupportedTypes() const {
    return [Type.LONG];
  }
}

///
unittest {
  auto time = new TimeMicrosLogicalType();
  assert(time.getName() == "time-micros");
  assert(time.validate(Type.LONG));
}

/**
   Represents a timestamp in milliseconds since the Unix epoch.

   The timestamp-millis logical type annotates an Avro long type.
   The long stores the number of milliseconds since the Unix epoch (1970-01-01 00:00:00.000 UTC).

   See_Also: https://avro.apache.org/docs/current/spec.html#Timestamp+%28millisecond+precision%29
*/
public class TimestampMillisLogicalType : LogicalType {
  /// Creates a timestamp-millis logical type.
  this() {
    super("timestamp-millis");
  }

  override
  public Type[] getSupportedTypes() const {
    return [Type.LONG];
  }
}

///
unittest {
  auto ts = new TimestampMillisLogicalType();
  assert(ts.getName() == "timestamp-millis");
  assert(ts.validate(Type.LONG));
}

/**
   Represents a timestamp in microseconds since the Unix epoch.

   The timestamp-micros logical type annotates an Avro long type.
   The long stores the number of microseconds since the Unix epoch (1970-01-01 00:00:00.000000 UTC).

   See_Also: https://avro.apache.org/docs/current/spec.html#Timestamp+%28microsecond+precision%29
*/
public class TimestampMicrosLogicalType : LogicalType {
  /// Creates a timestamp-micros logical type.
  this() {
    super("timestamp-micros");
  }

  override
  public Type[] getSupportedTypes() const {
    return [Type.LONG];
  }
}

///
unittest {
  auto ts = new TimestampMicrosLogicalType();
  assert(ts.getName() == "timestamp-micros");
  assert(ts.validate(Type.LONG));
}

/**
   Represents a local timestamp in milliseconds since the Unix epoch.

   The local-timestamp-millis logical type annotates an Avro long type.
   Unlike timestamp-millis, this represents local time without timezone information.

   See_Also: https://avro.apache.org/docs/current/spec.html#Local+timestamp+%28millisecond+precision%29
*/
public class LocalTimestampMillisLogicalType : LogicalType {
  /// Creates a local-timestamp-millis logical type.
  this() {
    super("local-timestamp-millis");
  }

  override
  public Type[] getSupportedTypes() const {
    return [Type.LONG];
  }
}

/**
   Represents a local timestamp in microseconds since the Unix epoch.

   The local-timestamp-micros logical type annotates an Avro long type.
   Unlike timestamp-micros, this represents local time without timezone information.

   See_Also: https://avro.apache.org/docs/current/spec.html#Local+timestamp+%28microsecond+precision%29
*/
public class LocalTimestampMicrosLogicalType : LogicalType {
  /// Creates a local-timestamp-micros logical type.
  this() {
    super("local-timestamp-micros");
  }

  override
  public Type[] getSupportedTypes() const {
    return [Type.LONG];
  }
}

/**
   Factory for creating logical types from their string names.

   This class provides a centralized way to parse logical type names
   and create the appropriate LogicalType instances.
*/
public class LogicalTypeFactory {
  /// Creates a logical type from its name and optional parameters.
  public static LogicalType create(string name, int precision = 0, int scale = 0) {
    switch (name) {
      case "decimal":
        if (precision <= 0) {
          throw new AvroRuntimeException("Decimal requires positive precision");
        }
        return new DecimalLogicalType(precision, scale);
      case "uuid":
        return new UuidLogicalType();
      case "date":
        return new DateLogicalType();
      case "time-millis":
        return new TimeMillisLogicalType();
      case "time-micros":
        return new TimeMicrosLogicalType();
      case "timestamp-millis":
        return new TimestampMillisLogicalType();
      case "timestamp-micros":
        return new TimestampMicrosLogicalType();
      case "local-timestamp-millis":
        return new LocalTimestampMillisLogicalType();
      case "local-timestamp-micros":
        return new LocalTimestampMicrosLogicalType();
      default:
        return null;
    }
  }
}

///
unittest {
  auto decimal = LogicalTypeFactory.create("decimal", 10, 2);
  assert(decimal !is null);
  assert(decimal.getName() == "decimal");

  auto date = LogicalTypeFactory.create("date");
  assert(date !is null);
  assert(date.getName() == "date");

  auto unknown = LogicalTypeFactory.create("unknown");
  assert(unknown is null);
}
