package moar;

public interface WokenWithData<T> {
  T find();

  WokenWithData<T> key(Runoneable<T> r);

  T upsert();

  T upsert(Runoneable<T> r);
}
