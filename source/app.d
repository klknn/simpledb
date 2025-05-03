/**
   # Design doc.

   ## Architecture

   Frontend:
   [SQL] -> Tokenizer -> Parser -> Codegen -> [Bytecode]

   Backend:
   [Bytecode] -> VM -> B-Tree -> Pager -> OS Interface
*/

import core.stdc.stdlib;
import core.stdc.stdio;
import core.sys.posix.stdio;

enum MetaCommandResult { SUCCESS, UNRECOGNIZED }
enum PrepareResult { SUCCESS, UNRECOGNIZED }
enum StatementType { INSERT, SELECT }

struct Statement {
  StatementType type;

  PrepareResult prepare(const(char)[] buffer) {
    if (buffer[0 .. 6] == "insert") {
      this.type = StatementType.INSERT;
      return PrepareResult.SUCCESS;
    }
    if (buffer == "select") {
      this.type = StatementType.SELECT;
      return PrepareResult.SUCCESS;
    }
    return PrepareResult.UNRECOGNIZED;
  }

  void execute() {
    final switch (this.type) {
    case StatementType.INSERT:
      printf("WIP INSERT\n");
      break;
    case StatementType.SELECT:
      printf("WIP SELECT\n");
      break;
    }
  }
}

MetaCommandResult do_meta_command(char[] buffer) {
  if (buffer == ".exit") {
    free(buffer.ptr);
    exit(EXIT_SUCCESS);
  } else {
    return MetaCommandResult.UNRECOGNIZED;
  }
}

void print_prompt() {
  printf("db > ");
}

char[] read_stdin() {
  char* ptr;
  size_t len;
  ssize_t bytes_read = getline(&ptr, &len, stdin);
  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }
  // Ignore trailing new line;
  ptr[bytes_read - 1] = 0;
  return ptr[0 .. bytes_read - 1];
}

extern (C)
int main() {
  while (true) {
    print_prompt();
    char[] input_buffer = read_stdin();
    scope(exit) free(input_buffer.ptr);

    if (input_buffer[0] == '.'){
      final switch (do_meta_command(input_buffer)) {
      case MetaCommandResult.SUCCESS:
        continue;
      case MetaCommandResult.UNRECOGNIZED:
        printf("Unrecognized command '%s'.\n", input_buffer.ptr);
        continue;
      }
    }

    Statement statement;
    final switch (statement.prepare(input_buffer)) {
    case PrepareResult.SUCCESS:
      break;
    case PrepareResult.UNRECOGNIZED:
      printf("Unrecognized keyword at start of %s.\n", input_buffer.ptr);
      continue;
    }
    statement.execute();
    printf("Executed.\n");
  }
  return EXIT_SUCCESS;
}
