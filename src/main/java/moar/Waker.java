package moar;
import static java.lang.String.format;
import static java.lang.String.join;
import static moar.Cost.$;
import static moar.Exceptional.require;
import static moar.JsonUtil.toJson;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import moar.WakeableRow.WithLongIdColumn;

public class Waker<T extends WakeableRow>
    implements
    WokenWithoutKey<T>,
    WokenWithoutData<T>,
    WokenWithData<T> {
  private static <T extends WakeableRow> WokeProxiedObject asWokeProxy(final T row) {
    return (WokeProxiedObject) row;
  }

  private static <T extends WakeableRow> T create(final Class<T> clz) {
    final ClassLoader c = Cost.class.getClassLoader();
    final Class<?>[] cc = { clz, WokeProxiedObject.class };
    return (T) Proxy.newProxyInstance(c, cc, new WokeProxy(clz));
  }

  private final Class<T> clz;

  private Session session;

  private Runoneable<T> key;

  public Waker(final Class<T> clz) {
    this.clz = clz;
  }

  private String buildSelect(final T woken) {
    final WokeProxiedObject wokenProxy = (WokeProxiedObject) woken;
    final List<String> columns = wokenProxy.$columns();
    String sql = "select\n" + join(",\n", columns);
    sql += ",\n";
    if (woken instanceof WakeableRow.WithoutIdColumn) {
      sql += "null as `id`";
    } else {
      sql += "`id`";
    }
    sql += "\n";
    return sql;
  }

  private void consumeResultSet(final Map<String, Object> map, final List<String> columns, final PreparedStatement ps)
      throws SQLException {
    try (ResultSet rs = ps.executeQuery()) {
      if (rs.next()) {
        mapResultRow(map, columns, rs);
      }
    }
  }

  private List<T> doFindAll(String tableish, final Object... params) throws SQLException {
    final List<T> result = new ArrayList<>();
    T woken = create(clz);
    if (tableish.contains("select")) {
      tableish = format("(%s) `x`", tableish);
    }
    String sql = buildSelect(woken);
    sql += "from " + tableish + "\n";
    try (ConnectionHold cn = session.reserve()) {
      try (PreparedStatement ps = cn.get().prepareStatement(sql)) {
        for (int i = 0; i < params.length; i++) {
          ps.setObject(i + 1, params[i]);
        }
        try (ResultSet rs = ps.executeQuery()) {
          WokeProxiedObject wokenProxy = (WokeProxiedObject) woken;
          final List<String> columns = wokenProxy.$columns();
          while (rs.next()) {
            final Map<String, Object> map = wokenProxy.$get();
            mapResultRow(map, columns, rs);
            wokenProxy.$set(map);
            result.add(woken);
            if (!rs.isLast()) {
              woken = create(clz);
              wokenProxy = (WokeProxiedObject) woken;
            }
          }
        }
      }
    }
    return result;
  }

  private T doSessionFind() throws SQLException {
    final T row = create(clz);
    synchronized (session) {
      String sessionKey = null;
      final WokeProxiedObject wokeProxyRow = asWokeProxy(row);
      key.run(row);
      sessionKey = toJson(row.getClass().getName(), wokeProxyRow.$get());
      final T existing = (T) session.map.get(sessionKey);
      if (existing != null) {
        return existing;
      }
      doTableFind(row);
      session.map.put(sessionKey, row);
      return row;
    }
  }

  private Object doSessionUpsert(final Runoneable<T> updator) {
    final T row = create(clz);
    synchronized (session) {
      String sessionKey = null;
      key.run(row);
      final WokeProxiedObject wokeProxyRow = asWokeProxy(row);
      sessionKey = toJson(row.getClass().getName(), wokeProxyRow.$get());
      final T existing = (T) session.map.get(sessionKey);
      if (existing != null) {
        asWokeProxy(row);
        if (row instanceof WakeableRow.WithLongIdColumn) {
          final WakeableRow.WithLongIdColumn rowWithId = (WithLongIdColumn) row;
          final WakeableRow.WithLongIdColumn existingWithId = (WithLongIdColumn) existing;
          rowWithId.setId(existingWithId.getId());
        }
      }
      updator.run(row);
      doTableUpsert(row);
      if (sessionKey != null) {
        session.map.put(sessionKey, row);
      }
    }
    return row;
  }

  private void doTableFind(final T row) {
    final WokeProxiedObject woke = (WokeProxiedObject) row;
    final String table = woke.$table();
    $(table, () -> {
      require(() -> doTableFindSql(row, table));
    });
  }

  private void doTableFindSql(final T row, final String table) throws SQLException {
    final WokeProxiedObject rowProxy = asWokeProxy(row);
    final Map<String, Object> map = rowProxy.$get();
    final List<String> columns = rowProxy.$columns();
    String sql = buildSelect(row);
    sql += "from " + table + " ";
    sql += "where ";
    int i = 0;
    for (final String key : map.keySet()) {
      if (i++ != 0) {
        sql += " and ";
      }
      sql += key + " = ?";
    }
    try (ConnectionHold cn = session.reserve()) {
      try (PreparedStatement ps = cn.get().prepareStatement(sql)) {
        setupStatement(map, map.keySet(), ps);
        consumeResultSet(map, columns, ps);
        rowProxy.$set(map);
      }
    }
  }

  private <T extends WakeableRow> void doTableUpsert(final T row) {
    final WokeProxiedObject woke = asWokeProxy(row);
    final String table = woke.$table();
    $(table, () -> {
      $("upsert", () -> require(() -> {
        doTableUpsertSql(woke, table);
      }));
    });
  }

  private void doTableUpsertSql(final WokeProxiedObject woke, final String table) throws SQLException {
    final List<String> columns = woke.$columns();
    final Map<String, Object> map = woke.$get();
    String sql = "insert into \n" + table + " (`id`,";
    sql += join("\n,", columns);
    sql += ") values (?,?";
    for (int i = 1; i < columns.size(); i++) {
      sql += "\n,?";
    }
    sql += "\n)\n";
    sql += " on duplicate key update `id`=last_insert_id(`id`) ";
    for (int i = 0; i < columns.size(); i++) {
      sql += "\n, ";
      sql += columns.get(i) + "=?";
    }
    final int[] identityColumn = { 1 };
    try (ConnectionHold hold = session.reserve()) {
      final Connection cn = hold.get();

      /* TODO: re-think sync on Waker.class. It's overkill and means that regardless of the number
       * of open connections we are only in this block one thread at a time across the application.
       * the obvious downside is the impact on the ability to scale but at the same time this is
       * the ultimate protection against two competing threads issuing database operations that
       * collide.
       *
       *  Especially with apps that use multiple different databases this is overkill.  We might
       *  want to sync on a mutex associated with the connection url or narrow it down to the
       *  moar session or just the connection. */
      synchronized (Waker.class) {
        try (PreparedStatement ps = cn.prepareStatement(sql, identityColumn)) {
          int p = 0;
          final String idColumn = "`id`";
          ps.setObject(++p, map.get(idColumn));
          for (int i = 0; i < columns.size(); i++) {
            final int i1 = i;
            ps.setObject(++p, map.get(columns.get(i1)));
          }
          for (int i = 0; i < columns.size(); i++) {
            final int i1 = i;
            ps.setObject(++p, map.get(columns.get(i1)));
          }
          try {
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
              if (rs.next()) {
                final Object id = rs.getObject(1);
                map.put(idColumn, id);
                woke.$set(map);
              }
            }
          } catch (final SQLSyntaxErrorException e) {
            throw new NonRetryableException(new JsonMessageException("bad sql syntax on upsert", e.getMessage(),
                stripTicks(sql), stripTicks(table)));
          } catch (final Throwable e) {
            throw new JsonMessageException("upsert failed", e.getMessage(), stripTicks(sql), stripTicks(table));
          }
        }
      }
    }
  }

  @Override
  public T find() {
    return (T) require(() -> doSessionFind());
  }

  @Override
  public List<T> findAll(final String tableish, final Object... params) {
    return require(() -> doFindAll(tableish, params));
  }

  @Override
  public WokenWithData<T> key(final Runoneable<T> r) {
    this.key = r;
    return this;
  }

  private void mapResultRow(final Map<String, Object> map, final List<String> columns, final ResultSet rs)
      throws SQLException {
    int i = 0;
    for (final String column : columns) {
      setColumnValue(map, column, rs.getObject(++i));
    }
    setColumnValue(map, "id", rs.getObject(++i));
  }

  @Override
  public WokenWithoutKey<T> of(final DataSource ds) {
    return of(new DataSourceSession(ds));
  }

  @Override
  public WokenWithoutKey<T> of(final Session cu) {
    this.session = cu;
    return this;
  }

  private void setColumnValue(final Map<String, Object> map, final String column, final Object value) {
    if (value == null) {
      map.remove(column);
    } else {
      map.put(column, value);
    }
  }

  private void setupStatement(final Map<String, Object> map, final Set<String> columns, final PreparedStatement ps)
      throws SQLException {
    int i = 0;
    for (final String column : columns) {
      ps.setObject(++i, map.get(column));
    }
  }

  private String stripTicks(final String string) {
    return string.replaceAll("\\`", "");
  }

  @Override
  public T upsert() {
    return upsert(r -> {});
  }

  @Override
  public T upsert(final Runoneable<T> updator) {
    return (T) require(() -> {
      return doSessionUpsert(updator);
    });
  }

}
