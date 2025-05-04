/**
   # Design doc.

   ## Architecture

   Frontend:
   [SQL] -> Tokenizer -> Parser -> Codegen -> [Bytecode]

   Backend:
   [Bytecode] -> VM -> B-Tree -> Pager -> OS Interface
*/
import core.stdc.string;
import core.stdc.stdlib;
import core.stdc.stdio;

enum MetaCommandResult { SUCCESS, UNRECOGNIZED }
enum PrepareResult { SUCCESS, UNRECOGNIZED, SYNTAX_ERROR, STRING_TOO_LONG, NEGATIVE_ID }
enum StatementType { INSERT, SELECT }

struct Row {
  uint id;
  char[32] username;
  char[255] email;

  void serialize(void* dst) const {
    static foreach (src; this.tupleof) {
      memcpy(dst + src.offsetof, &src, src.sizeof);
    }
  }

  void deserialize(const void* src) {
    static foreach (dst; this.tupleof) {
      memcpy(&dst, src + dst.offsetof, dst.sizeof);
    }
  }

  void print() const {
    printf("(%d, %s, %s)\n", id, username.ptr, email.ptr);
  }
}

enum TotalFieldSizeOf(T) = {
  size_t n;
  foreach (F; T.init.tupleof) n += F.sizeof;
  return n;
}();

@("Row test")
unittest {
  assert(Row.id.sizeof == 4);
  assert(Row.username.sizeof == 32);
  assert(Row.email.sizeof == 255);
  assert(Row.id.offsetof == 0);
  assert(Row.username.offsetof == 4);
  assert(Row.email.offsetof == 4 + 32);
  static assert(TotalFieldSizeOf!Row == 4 + 32 + 255);

  Row src = {id: 1, username: "cstack", email: "foo@bar.com"};
  char[TotalFieldSizeOf!Row] buf;
  src.serialize(buf.ptr);
  Row dst;
  dst.deserialize(buf.ptr);
  assert(dst.id == 1);
  assert(strcmp(dst.username.ptr, src.username.ptr) == 0);
  assert(strcmp(dst.email.ptr, src.email.ptr) == 0);
}

enum uint PAGE_SIZE = 4096;
// enum uint TABLE_MAX_PAGES = 100;
enum uint ROWS_PER_PAGE = PAGE_SIZE / TotalFieldSizeOf!Row;

enum ExecuteResult { SUCCESS, TABLE_FULL }

struct Table {
  uint num_rows;
  void*[] pages;

  size_t max_rows() const {
    return ROWS_PER_PAGE * pages.length;
  }

  void allocate(size_t max_pages) {
    void** p = cast(void**) malloc(max_pages * (void*).sizeof);
    this.pages = p[0 .. max_pages];
    this.pages[] = null;
  }

  ~this() {
    foreach (p; this.pages) {
      if (p is null) break;
      free(p);
    }
  }

  void* row_slot(uint row_idx) {
    uint page_idx = row_idx / ROWS_PER_PAGE;
    void* page = this.pages[page_idx];
    if (page == null) {
      // Allocate memory only when we try to access page.
      page = this.pages[page_idx] = malloc(PAGE_SIZE);
    }
    uint row_offset = row_idx % ROWS_PER_PAGE;
    ptrdiff_t byte_offset = row_offset * TotalFieldSizeOf!Row;
    return page + byte_offset;
  }

  ExecuteResult insert(ref const Row row) {
    if (this.num_rows >= this.max_rows) return ExecuteResult.TABLE_FULL;
    row.serialize(this.row_slot(this.num_rows));
    ++this.num_rows;
    return ExecuteResult.SUCCESS;
  }

  ExecuteResult select() {
    Row row;
    foreach (i; 0 .. this.num_rows) {
      row.deserialize(this.row_slot(i));
      row.print();
    }
    return ExecuteResult.SUCCESS;
  }
}

@("Table test")
unittest {
  Table table;
  table.allocate(1);
  Row row;
  foreach (_; 0 .. ROWS_PER_PAGE) {
    assert(table.insert(row) == ExecuteResult.SUCCESS);
  }
  assert(table.insert(row) == ExecuteResult.TABLE_FULL);
}

struct Statement {
  StatementType type;
  Row row_to_insert;

  PrepareResult prepare_insert(const(char)[] buffer) {
    this.type = StatementType.INSERT;
    size_t len = strlen(buffer.ptr) + 1;
    char* buf = cast(char*) malloc(len);
    memcpy(buf, buffer.ptr, len);
    char* keyword = strtok(buf, " ");
    assert(strcmp(keyword, "insert") == 0);
    char* id_string = strtok(null, " ");
    char* username = strtok(null, " ");
    char* email = strtok(null, " ");
    if (id_string is null || username is null || email is null) {
      return PrepareResult.SYNTAX_ERROR;
    }
    int id = atoi(id_string);
    if (id < 0) return PrepareResult.NEGATIVE_ID;
    if (strlen(username) >= Row.username.length) {
      return PrepareResult.STRING_TOO_LONG;
    }
    if (strlen(email) >= Row.email.length) {
      return PrepareResult.STRING_TOO_LONG;
    }

    this.row_to_insert.id = id;
    strcpy(this.row_to_insert.username.ptr, username);
    strcpy(this.row_to_insert.email.ptr, email);
    return PrepareResult.SUCCESS;
  }

  PrepareResult prepare(const char[] buffer) {
    if (strncmp(buffer.ptr, "insert", 6) == 0) {
      return this.prepare_insert(buffer);
    }
    if (strcmp(buffer.ptr, "select") == 0) {
      this.type = StatementType.SELECT;
      return PrepareResult.SUCCESS;
    }
    return PrepareResult.UNRECOGNIZED;
  }

  ExecuteResult execute(ref Table table) {
    final switch (this.type) {
      case StatementType.INSERT:
        return table.insert(this.row_to_insert);
      case StatementType.SELECT:
        return table.select();
    }
  }
}

@("Statement test")
unittest {
  Statement statement;
  assert(statement.prepare("select") == PrepareResult.SUCCESS);

  assert(statement.prepare("insert") == PrepareResult.SYNTAX_ERROR);
  assert(statement.prepare("insert 1 cstack foo@bar.com") == PrepareResult.SUCCESS);
  assert(statement.row_to_insert.id == 1);
  assert(strcmp(statement.row_to_insert.username.ptr, "cstack") == 0);
  assert(strcmp(statement.row_to_insert.email.ptr, "foo@bar.com") == 0);

  assert(statement.prepare("insert -1 cstack foo@bar.com") == PrepareResult.NEGATIVE_ID);

  enum int name_offset = "insert 1 ".length;
  enum int name_len = Row.username.sizeof;
  enum int email_len = Row.email.sizeof;
  char[name_offset + name_len + 1 + email_len + 1] long_query = "insert 1 ";
  long_query[name_offset .. name_offset + name_len] = 'a';
  long_query[name_offset + name_len] = ' ';
  long_query[name_offset + name_len + 1 .. $] = 'b';
  long_query[$-1] = 0;
  assert(statement.prepare(long_query) == PrepareResult.STRING_TOO_LONG);
}

MetaCommandResult do_meta_command(char[] buffer) {
  if (strcmp(buffer.ptr, ".exit") == 0) {
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
  int len = 255;
  char* ptr = cast(char*) malloc(len);
  // size_t len;
  // auto bytes_read = getline(&ptr, &len, stdin);
  if (fgets(ptr, len, stdin) is null) { // bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }
  // Ignore trailing new line;
  // ptr[bytes_read - 1] = 0;
  size_t slen = strlen(ptr);
  ptr[slen-1] = 0;
  return ptr[0 .. slen]; // ptr[0 .. bytes_read - 1];
}

@("Assert true test")
unittest {
  assert(true, "this should pass.");
}

version (unittest) {

  private template from(string modname) {
    mixin("import from = ", modname, ";");
  }
  extern (C) int main(int argc, const char** argv) {
    import core.stdc.stdio;
    import std.traits;
    static foreach (mod; ["app"]) {
      static foreach (test; __traits(getUnitTests, from!mod)) {
        {
          auto udas = getUDAs!(test, string);
          static if (udas.length) {
            printf("unittest: [%s] %s ... ", mod.ptr, udas[0].ptr);
          } else {
            printf("unittest: [%s] %s ... ", mod.ptr, test.stringof.ptr);
          }
          test();
          printf("OK\n");
        }
      }
    }
    printf("Test completed!\n");
    return 0;
  }

} else {
  extern (C)
      int main() {
    Table table;
    table.allocate(100);
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
        case PrepareResult.SYNTAX_ERROR:
          printf("Syntax error. Could not parse statement.\n");
          continue;
        case PrepareResult.NEGATIVE_ID:
          printf("ID must be positive.\n");
          continue;
        case PrepareResult.STRING_TOO_LONG:
          printf("String is too long.\n");
          continue;
        case PrepareResult.UNRECOGNIZED:
          printf("Unrecognized keyword at start of %s.\n", input_buffer.ptr);
          continue;
      }

      final switch (statement.execute(table)) {
        case ExecuteResult.SUCCESS:
          printf("Executed.\n");
          break;
        case ExecuteResult.TABLE_FULL:
          printf("Error: Table full.\n");
          break;
      }
    }
    return EXIT_SUCCESS;
  }
}
