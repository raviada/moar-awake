package moar;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Session {
  Map<String, Object> map = new ConcurrentHashMap<>();

  public abstract ConnectionHold reserve();

}
