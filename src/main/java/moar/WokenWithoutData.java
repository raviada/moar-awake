package moar;

import javax.sql.DataSource;

public interface WokenWithoutData<T extends WakeableRow> {
  WokenWithoutKey<T> of(DataSource ds);

  WokenWithoutKey<T> of(Session s);
}
