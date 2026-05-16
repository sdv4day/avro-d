/**
   Protocol wire format for Avro RPC.

   Avro RPC provides a remote procedure call mechanism using Avro for serialization.
   The protocol defines messages with requests and responses, transmitted over
   various transports (HTTP, WebSocket, etc.).

   See_Also: https://avro.apache.org/docs/current/spec.html#Protocol+Wire+Format
*/
module avro.protocol;

import std.conv : to;
import std.json : JSONValue, parseJSON;
import std.array : appender;

import avro.schema : Schema;
import avro.type : Type;
import avro.exception : AvroRuntimeException;

@safe:

/**
   Represents a message in an Avro protocol.

   A message consists of:
   - A name
   - A request schema (the parameters)
   - An optional response schema (the return type)
   - An optional error schema (the error types)
*/
public class Message {
  private string name;
  private Schema request;
  private Schema response;
  private Schema[] errors;

  /// Creates a new message.
  this(string name, Schema request, Schema response = null, Schema[] errors = null) {
    this.name = name;
    this.request = request;
    this.response = response;
    this.errors = errors ? errors : [];
  }

  /// Returns the message name.
  public string getName() const {
    return name;
  }

  /// Returns the request schema.
  public Schema getRequest() {
    return request;
  }

  /// Returns the response schema.
  public Schema getResponse() {
    return response;
  }

  /// Returns the error schemas.
  public Schema[] getErrors() {
    return errors;
  }
}

/**
   Represents an Avro protocol.

   A protocol defines:
   - A namespace and name
   - A set of types (schemas)
   - A set of messages (RPC methods)
*/
public class Protocol {
  private string name;
  private string namespace_;
  private string doc;
  private Schema[string] types;
  private Message[string] messages;

  /// Creates a new protocol.
  this(string name, string namespace_ = null, string doc = null) {
    this.name = name;
    this.namespace_ = namespace_;
    this.doc = doc;
  }

  /// Returns the protocol name.
  public string getName() const {
    return name;
  }

  /// Returns the protocol namespace.
  public string getNamespace() const {
    return namespace_;
  }

  /// Returns the protocol documentation.
  public string getDoc() const {
    return doc;
  }

  /// Adds a type (schema) to the protocol.
  public void addType(string name, Schema schema) {
    types[name] = schema;
  }

  /// Returns a type by name.
  public Schema getType(string name) {
    return name in types ? types[name] : null;
  }

  /// Adds a message to the protocol.
  public void addMessage(string name, Message message) {
    messages[name] = message;
  }

  /// Returns a message by name.
  public Message getMessage(string name) {
    return name in messages ? messages[name] : null;
  }

  /// Returns all messages.
  public Message[string] getMessages() {
    return messages;
  }
}

///
unittest {
  auto protocol = new Protocol("TestProtocol", "com.example");
  assert(protocol.getName() == "TestProtocol");
  assert(protocol.getNamespace() == "com.example");
}

/**
   Parser for Avro protocol definitions in JSON format.
*/
public class ProtocolParser {
  /// Parses a protocol from JSON text.
  public Protocol parse(string text) {
    JSONValue json = parseJSON(text);
    return parseJson(json);
  }

  /// Parses a protocol from a JSON value.
  public Protocol parseJson(JSONValue json) {
    string name = json["protocol"].str();
    string namespace_ = "namespace" in json ? json["namespace"].str() : null;
    string doc = "doc" in json ? json["doc"].str() : null;

    auto protocol = new Protocol(name, namespace_, doc);

    return protocol;
  }
}

/**
   RPC error representation.

   Errors in Avro RPC are transmitted as a union of error types.
*/
public class RpcError {
  private string message;
  private Schema errorSchema;

  /// Creates a new RPC error.
  this(string message, Schema errorSchema = null) {
    this.message = message;
    this.errorSchema = errorSchema;
  }

  /// Returns the error message.
  public string getMessage() const {
    return message;
  }

  /// Returns the error schema.
  public Schema getErrorSchema() {
    return errorSchema;
  }
}

/**
   Base class for RPC transports.

   Transports handle the actual transmission of RPC frames.
   Implementations include HTTP, WebSocket, and custom transports.
*/
public abstract class RpcTransport {
  /// Sends a request and returns the response.
  public abstract ubyte[] send(ubyte[] request);

  /// Receives a request (server-side).
  public abstract ubyte[] receive();

  /// Closes the transport.
  public abstract void close();
}

/**
   RPC client for making remote procedure calls.
*/
public class RpcClient {
  private Protocol protocol;
  private RpcTransport transport;

  /// Creates a new RPC client.
  this(Protocol protocol, RpcTransport transport) {
    this.protocol = protocol;
    this.transport = transport;
  }

  /// Calls a remote method.
  public ubyte[] call(string methodName, ubyte[] requestData) {
    Message msg = protocol.getMessage(methodName);
    if (msg is null) {
      throw new AvroRuntimeException("Unknown method: " ~ methodName);
    }

    return transport.send(requestData);
  }
}

/**
   RPC server for handling remote procedure calls.
*/
public class RpcServer {
  private Protocol protocol;
  private RpcTransport transport;

  /// Creates a new RPC server.
  this(Protocol protocol, RpcTransport transport) {
    this.protocol = protocol;
    this.transport = transport;
  }

  /// Starts the server.
  public void start() {
  }

  /// Stops the server.
  public void stop() {
    transport.close();
  }
}
