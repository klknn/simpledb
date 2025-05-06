module dspec;

import std.array : replace;
import std.conv : text;
import std.logger : info, error;

struct DSpec {
  string what;
  TestCase[] cases;
  void delegate() before_block;

  @disable this(this);

  ~this() {
    foreach (ref c; cases) {
      info(what ~ " " ~ c.name);
      this.before_block();
      c.block();
    }
  }

  ref DSpec before(void delegate() block) {
    this.before_block = block;
    return this;
  }

  ref DSpec it(string name, void delegate() block) {
    cases ~= TestCase(name, block);
    return this;
  }
}

alias describe = DSpec;

struct TestCase {
  string name;
  void delegate() block;
}

bool match_array(string[] actual, string[] expect) {
  string mismatched;
  foreach (i, ex; expect) {
    if (actual.length < i) {
      mismatched ~= i"\nActual[$(i)]: NOT FOUND\nExpect[$(i)]: \"$(ex)\"\n".text;
      continue;
    }
    string ac = actual[i].replace("\r", ""); // for windows.
    if (ac == ex)
      continue;
    mismatched ~= i"\nActual[$(i)]: \"$(ac)\"\nExpect[$(i)]: \"$(ex)\"\n".text;
  }
  if (mismatched != "") {
    error("Mismatched elements:\n" ~ mismatched);
    return false;
  }
  return true;
}
