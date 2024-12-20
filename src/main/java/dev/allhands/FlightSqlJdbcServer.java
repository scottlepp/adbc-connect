package dev.allhands;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class FlightSqlJdbcServer implements FlightProducer, AutoCloseable {
    private final BufferAllocator allocator;
    private Connection jdbcConnection;

    public FlightSqlJdbcServer(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    public void setJdbcConnection(String jdbcUrl) throws SQLException {
        if (jdbcConnection != null) {
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                // Ignore close errors
            }
        }
        jdbcConnection = DriverManager.getConnection(jdbcUrl);
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        try {
            byte[] ticketBytes = ticket.getBytes();
            String query = new String(ticketBytes);

            try (PreparedStatement stmt = jdbcConnection.prepareStatement(query);
                 ResultSet rs = stmt.executeQuery()) {

                ResultSetMetaData metaData = rs.getMetaData();
                List<Field> fields = new ArrayList<>();
                
                // Create schema from result set metadata
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    int sqlType = metaData.getColumnType(i);
                    
                    ArrowType arrowType = mapSqlTypeToArrowType(sqlType);
                    fields.add(new Field(columnName, FieldType.nullable(arrowType), null));
                }

                Schema schema = new Schema(fields);
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                listener.start(root);

                int rowCount = 0;
                final int batchSize = 1000;

                while (rs.next()) {
                    if (rowCount == 0) {
                        root.allocateNew();
                    }

                    for (int i = 0; i < fields.size(); i++) {
                        FieldVector vector = root.getVector(i);
                        setVectorValue(vector, rowCount, rs, i + 1);
                    }

                    rowCount++;

                    if (rowCount == batchSize) {
                        root.setRowCount(rowCount);
                        listener.putNext();
                        rowCount = 0;
                    }
                }

                if (rowCount > 0) {
                    root.setRowCount(rowCount);
                    listener.putNext();
                }

                root.close();
                listener.completed();
            }
        } catch (Exception e) {
            listener.error(e);
        }
    }

    private ArrowType mapSqlTypeToArrowType(int sqlType) {
        switch (sqlType) {
            case Types.INTEGER:
                return new ArrowType.Int(32, true);
            case Types.BIGINT:
                return new ArrowType.Int(64, true);
            case Types.DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case Types.FLOAT:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.LONGVARCHAR:
                return new ArrowType.Utf8();
            case Types.DATE:
                return new ArrowType.Date(DateUnit.DAY);
            case Types.TIMESTAMP:
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            case Types.BOOLEAN:
                return new ArrowType.Bool();
            default:
                return new ArrowType.Utf8(); // Default to string for unsupported types
        }
    }

    private void setVectorValue(FieldVector vector, int index, ResultSet rs, int columnIndex) throws SQLException {
        if (rs.getObject(columnIndex) == null) {
            vector.setNull(index);
            return;
        }

        if (vector instanceof IntVector) {
            ((IntVector) vector).setSafe(index, rs.getInt(columnIndex));
        } else if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).setSafe(index, rs.getLong(columnIndex));
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).setSafe(index, rs.getDouble(columnIndex));
        } else if (vector instanceof Float4Vector) {
            ((Float4Vector) vector).setSafe(index, rs.getFloat(columnIndex));
        } else if (vector instanceof VarCharVector) {
            ((VarCharVector) vector).setSafe(index, rs.getString(columnIndex).getBytes());
        } else if (vector instanceof DateDayVector) {
            java.sql.Date date = rs.getDate(columnIndex);
            ((DateDayVector) vector).setSafe(index, (int) (date.getTime() / (86400000L)));
        } else if (vector instanceof TimeStampMicroVector) {
            Timestamp ts = rs.getTimestamp(columnIndex);
            ((TimeStampMicroVector) vector).setSafe(index, ts.getTime() * 1000);
        } else if (vector instanceof BitVector) {
            ((BitVector) vector).setSafe(index, rs.getBoolean(columnIndex) ? 1 : 0);
        }
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        try {
            byte[] bytes = descriptor.getCommand();
            String query = new String(bytes);

            // Create a ticket containing the query
            Ticket ticket = new Ticket(query.getBytes());

            // Create a dummy schema for the FlightInfo
            List<Field> fields = Collections.singletonList(
                Field.nullable("dummy", new ArrowType.Int(32, true))
            );
            Schema schema = new Schema(fields);

            Location location = Location.forGrpcInsecure("localhost", 47470);
            FlightEndpoint endpoint = new FlightEndpoint(ticket, location);

            return new FlightInfo(
                schema,
                descriptor,
                Collections.singletonList(endpoint),
                -1,
                -1
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        listener.onCompleted();
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        listener.onCompleted();
    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        listener.onCompleted();
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> listener) {
        listener.onError(new UnsupportedOperationException("Put operation not supported"));
        return () -> {};
    }

    @Override
    public void close() throws Exception {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: java -jar arrow-flightsql-jdbc.jar <jdbc-url>");
            System.exit(1);
        }

        String jdbcUrl = args[0];
        int port = 47470;

        try (BufferAllocator allocator = new RootAllocator();
             FlightSqlJdbcServer producer = new FlightSqlJdbcServer(allocator)) {

            producer.setJdbcConnection(jdbcUrl);

            Location location = Location.forGrpcInsecure("0.0.0.0", port);
            FlightServer server = FlightServer.builder()
                .allocator(allocator)
                .location(location)
                .producer(producer)
                .build();

            server.start();
            System.out.println("Server listening on port " + port);
            server.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}