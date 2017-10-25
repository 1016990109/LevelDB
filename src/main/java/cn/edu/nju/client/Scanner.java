package cn.edu.nju.client;

import cn.edu.nju.file.Key;

import java.util.Comparator;

public interface Scanner {
  public static final Comparator<Key> ALL = new Comparator<Key>() {
    @Override
    public int compare(Key o1, Key o2) {
      return o1.compareTo(o2);
    }
  };
}