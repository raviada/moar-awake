package moar;

public interface WakeableRow {
  interface WithLongIdColumn
      extends
      WakeableRow {
    Long getId();

    void setId(Long id);
  }

  interface WithoutIdColumn
      extends
      WakeableRow {}

}
