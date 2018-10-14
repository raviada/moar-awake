package moar;

public class WakeUp {
  public static <T extends WakeableRow> WokenWithoutData<T> wake(final Class<T> clz) {
    return new Waker<>(clz);
  }
}
