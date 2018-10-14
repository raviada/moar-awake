package moar;
import java.sql.Connection;

public class ConnectionSession
    extends
    Session
    implements
    AutoCloseable {

  private final Connection connection;

  public ConnectionSession(final Connection cn) {
    connection = cn;
  }

  @Override
  public void close() throws Exception {
    // Connection is intentionally left open because we don't own it and
    // should not close it.
  }

  @Override
  public ConnectionHold reserve() {
    return new ConnectionHold() {

      @Override
      public void close() {
        // intentionally blank because we are a connection session and
        // keep the connection under our control since it was given to
        // us.  The code that created us owns the connection and should
        // close it.
      }

      @Override
      public Connection get() {
        return connection;
      }
    };
  }

}
