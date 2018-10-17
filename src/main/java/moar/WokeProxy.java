package moar;
import static com.google.common.reflect.TypeToken.of;
import static java.lang.String.format;
import static java.lang.String.join;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import moar.WakeableRow.WithLongIdColumn;

public class WokeProxy
    implements
    InvocationHandler {
  private static String toDbName(final String string) {
    final String[] nameParts = string.split("(?=\\p{Upper})");
    return format("`%s`", join("_", nameParts).toLowerCase());
  }

  final Map<String, Object> map = new ConcurrentHashMap<>();
  final Class<? extends WakeableRow> clz;

  public WokeProxy(final Class<? extends WakeableRow> clz) {
    this.clz = clz;
  }

  private List<String> $columns() {
    final List<String> columns = new ArrayList<>();
    for (final Method method : clz.getMethods()) {
      final String name = method.getName();
      if (isProperty(name)) {
        if (!getPropertyName(name).equals("Id")) {
          final String dbName = toDbName(getPropertyName(name));
          if (!columns.contains(dbName)) {
            columns.add(dbName);
          }
        }
      }
    }
    return columns;
  }

  private Object $get() {
    final Map<String, Object> dbMap = new ConcurrentHashMap<>();
    for (final String key : map.keySet()) {
      final String dbName = toDbName(key);
      dbMap.put(dbName, map.get(key));
    }
    return dbMap;
  }

  private Object $set(final Object[] args) {
    final Map<String, Object> dbMap = (Map<String, Object>) args[0];
    for (final String key : dbMap.keySet()) {
      map.put(fromDbName(key), dbMap.get(key));
    }
    return dbMap;
  }

  private String fromDbName(final String key) {
    final StringBuilder s = new StringBuilder();
    boolean upper = true;
    for (final char c : key.toCharArray()) {
      if (c == '`') {
        // ignore
      } else if (c != '_') {
        if (upper) {
          s.append(Character.toUpperCase(c));
          upper = false;
        } else {
          s.append(c);
        }
      }
      if (c == '_') {
        upper = true;
      }
    }
    return s.toString();
  }

  private Object getProperty(final String name) {
    return map.get(getPropertyName(name));
  }

  private String getPropertyName(final String name) {
    return name.substring("get".length());
  }

  @Override
  public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
    final String name = method.getName();
    if (args == null) {
      if (name.startsWith("$")) {
        if (name.equals("$table")) {
          return toDbName(clz.getSimpleName());
        } else if (name.equals("$columns")) {
          return $columns();
        } else if (name.equals("$get")) {
          return $get();
        }
      } else {
        if (isProperty(name)) {
          Object value = getProperty(name);
          if (value instanceof BigInteger) {
            if (method.getReturnType() == Long.class || isLongIdColumn(name)) {
              value = ((BigInteger) value).longValue();
            }
          }
          return value;
        }
      }
    } else if (args.length == 1) {
      if (name.startsWith("$")) {
        if (name.equals("$set")) {
          return $set(args);
        }
      } else {
        if (isProperty(name)) {
          setProperty(name, args[0]);
          return null;
        }
      }
    }
    throw new StringMessageException(name + " is not supported by this proxy");
  }

  private boolean isLongIdColumn(final String name) {
    if (!name.equals("getId")) {
      return false;
    }
    final boolean isLongIdColumn = of(clz).isSubtypeOf(of(WithLongIdColumn.class));
    return isLongIdColumn;
  }

  private boolean isProperty(final String name) {
    // try to be fast with this check!
    // get or set!
    return name.startsWith("g") || name.startsWith("s") && name.substring(1).startsWith("et");
  }

  private void setProperty(final String name, final Object arg) {
    if (arg == null) {
      map.remove(name);
    } else {
      map.put(getPropertyName(name), arg);
    }
  }
}
