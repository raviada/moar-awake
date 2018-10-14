package moar;

import java.util.List;

public interface WokenWithoutKey<T extends WakeableRow> {
  List<T> findAll(String queryOrTable, Object... params);

  WokenWithData<T> key(Runoneable<T> r);

}
