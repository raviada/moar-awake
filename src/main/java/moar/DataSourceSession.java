package moar;

import static moar.Exceptional.require;
import java.sql.Connection;
import javax.sql.DataSource;

public class DataSourceSession
    extends
    Session {

  private final DataSource ds;

  public DataSourceSession(final DataSource ds) {
    this.ds = ds;
  }

  @Override
  public ConnectionHold reserve() {
    final Connection cn = require(() -> ds.getConnection());
    return new ConnectionHold() {

      @Override
      public void close() {
        require(() -> cn.close());
      }

      @Override
      public Connection get() {
        return cn;
      }
    };
  }

}
