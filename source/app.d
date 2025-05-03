/**
  # Design doc.

  ## Architecture

  Frontend:
  [SQL] -> Tokenizer -> Parser -> Codegen -> [Bytecode]

  Backend:
  [Bytecode] -> VM -> B-Tree -> Pager -> OS Interface
 */
import core.memory;
import core.stdc.stdlib;
import core.stdc.stdio;
import core.sys.posix.stdio;

@trusted
void print_prompt() {
  printf("db > ");
}

struct InputBuffer {
  char[] buffer;

  @safe
  ~this() {
    pureFree(&this.buffer[0]);
  }
}


InputBuffer read_stdin() {
  char* ptr;
  size_t len;
  ssize_t bytes_read = getline(&ptr, &len, stdin);
  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }
  // Ignore trailing new line;
  ptr[bytes_read - 1] = 0;
  InputBuffer ret;
  ret.buffer = ptr[0 .. bytes_read - 1];
  return ret;
}


extern (C) @safe
int main() {
  while (true) {
    print_prompt();
    InputBuffer input_buffer = read_stdin();

    if (input_buffer.buffer == ".exit"){
      return EXIT_SUCCESS;
    } else {
      printf("Unrecognized command '%s'.\n", input_buffer.buffer.ptr);
    }
  }
}
