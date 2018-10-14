package moar;

import java.util.List;
import java.util.Map;

interface WokeProxiedObject {
  List<String> $columns();

  Map<String, Object> $get();

  void $set(Map<String, Object> map);

  String $table();

  WokeProxy self();
}
