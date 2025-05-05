import std.process;
import std.stdio;
import std.string;
import std.file;
import std.array : array;
import std.algorithm : map;
import std.logger : info, error;
import std.conv : text;
import std.range : repeat;

enum RESET = "\033[0m";
enum RED = "\033[31m";
enum GREEN = "\033[32m";

string[] run_script(string[] commands) {
  ProcessPipes p = pipeProcess(["./simpledb", "test.db"]);
  scope (exit)
    assert(wait(p.pid) == 0);
  foreach (cmd; commands) {
    p.stdin.writeln(cmd);
  }
  p.stdin.close();
  return array(p.stdout.byLine.map!idup);
}

struct Test {
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

  ref Test before(void delegate() block) {
    this.before_block = block;
    return this;
  }

  ref Test it(string name, void delegate() block) {
    cases ~= TestCase(name, block);
    return this;
  }
}

alias describe = Test;

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

void main() {
  info("cwd: ", getcwd);
  info("building repl");
  auto dub_build = execute(["dub", "build"]);
  assert(dub_build.status == 0);
  info(dub_build.output);

  describe("database") // @suppress(dscanner.unused_result)
    .before({
      if (exists("test.db")) {
        std.file.remove("test.db");
      }
    })

    .it("exits successfully", { assert(run_script([".exit"]) == ["db > "]); })

    .it("inserts and retrieves a row",
    {
      string[] result = run_script([
        "insert 1 user1 person1@example.com", "select", ".exit"
      ]);
      assert(match_array(result, [
        "db > Executed.",
        "db > (1, user1, person1@example.com)",
        "Executed.",
        "db > "
      ]));
    }) // .it("prints error message when table is full",
    // {
    //   version (Windows)
    //     return; // Super slow due to pipe?

    //   string[] script;
    //   foreach (i; 0 .. 1401) {
    //     script ~= i"insert $(i) user$(i) person$(i)@example.com".text;
    //   }
    //   script ~= ".exit";
    //   auto result = run_script(script);
    //   assert(match_array([result[$ - 2]], ["db > Error: Table full."]));
    // })
    .it("allows inserting strings that are the maximum length",
    {
      string long_username = 'a'.repeat(32).text;
      string long_email = 'b'.repeat(255).text;
      string insert = i"insert 1 $(long_username) $(long_email)".text;
      string[] result = run_script([insert, "select", ".exit"]);
      assert(match_array(result, [
        "db > Executed.",
        i"db > (1, $(long_username), $(long_email))".text,
        "Executed.",
        "db > ",
      ]));
    })

    .it("prints error message if strings are too long",
    {
      string long_username = 'a'.repeat(33).text;
      string long_email = 'b'.repeat(256).text;
      string insert = i"insert 1 $(long_username) $(long_email)".text;
      string[] result = run_script([insert, "select", ".exit"]);
      assert(match_array(result, [
        "db > String is too long.",
        "db > Executed.",
        "db > ",
      ]));
    })

    .it("prints an error message if id is negative",
    {
      assert(match_array(run_script([
        "insert -1 cstack foo@bar.com", "select", ".exit"
      ]), [
        "db > ID must be positive.",
        "db > Executed.",
        "db > "
      ]));
    })

    .it("keeps data after closing connection", {
      string[] result1 = run_script([
        "insert 1 user1 person1@example.com", ".exit"
      ]);
      assert(match_array(result1, ["db > Executed.", "db > "]));

      string[] result2 = run_script(["select", ".exit"]);
      assert(match_array(result2, [
        "db > (1, user1, person1@example.com)", "Executed.", "db > "
      ]));
    });

  scope (success)
    info(GREEN ~ "=== TEST PASSED 😁 ===" ~ RESET);
  scope (failure)
    error(RED ~ "=== TEST FAILED 😭 ===" ~ RESET);
}
