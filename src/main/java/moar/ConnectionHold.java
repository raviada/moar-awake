package moar;

import java.sql.Connection;

public interface ConnectionHold
    extends
    AutoCloseable {

  @Override
  void close();

  Connection get();

}
