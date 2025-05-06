module repl_test;

import dspec : DSpec, match_array;
import std.process : execute, pipeProcess, ProcessPipes, wait;
import std.file : exists, getcwd, remove;
import std.array : array;
import std.algorithm : each, map;
import std.logger : info, error;
import std.conv : text;
import std.range : repeat;

enum RESET = "\033[0m";
enum RED = "\033[31m";
enum GREEN = "\033[32m";

string[] run_script(string[] commands) {
  ProcessPipes p = pipeProcess(["./simpledb", "test.db"]);
  scope (exit)
    wait(p.pid);
  commands.each!(x => p.stdin.writeln(x));
  p.stdin.close();
  return p.stdout.byLine.map!idup.array;
}

void main() {
  info("cwd: ", getcwd);
  info("building repl");
  auto dub_build = execute(["dub", "build"]);
  assert(dub_build.status == 0, dub_build.output);

  DSpec("database")
    .before({
      if (exists("test.db")) {
        remove("test.db");
      }
    })

    .it("exits successfully", {
      assert(match_array(run_script([".exit"]), ["db > "]));
    })

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
    })

    .it("prints error message when table is full",
    {
      version (Windows)
        return; // Super slow due to pipe?

      string[] script;
      foreach (i; 0 .. 1401) {
        script ~= i"insert $(i) user$(i) person$(i)@example.com".text;
      }
      script ~= ".exit";
      auto result = run_script(script);
      assert(match_array([result[$ - 2]], ["db > Error: Table full."]));
    })

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
    info("\n" ~ GREEN ~ "=== REPL TEST PASSED 😉 ===" ~ RESET);
  scope (failure)
    error("\n" ~ RED ~ "=== REPL TEST FAILED 😭 ===" ~ RESET);
}
