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
import core.stdc.stdint;
import core.stdc.errno;

align(1) struct LeafNode {
  align(1) struct Header {
  align(1):
    ubyte node_type;
    bool is_root;
    uint parent_pointer;
  }

  align(1) struct Cell {
    uint key;
    void[ROW_SIZE] value;
  }

  enum SPACE_FOR_CELLS = PAGE_SIZE - Header.sizeof - uint.sizeof;
  enum MAX_CELLS = SPACE_FOR_CELLS / Cell.sizeof;

align(1):
  Header header;
  uint num_cells;
  Cell[MAX_CELLS] cells;
}

enum MetaCommandResult {
  SUCCESS,
  UNRECOGNIZED
}

enum PrepareResult {
  SUCCESS,
  UNRECOGNIZED,
  SYNTAX_ERROR,
  STRING_TOO_LONG,
  NEGATIVE_ID
}

enum StatementType {
  INSERT,
  SELECT
}

struct Row {
  uint id;
  char[33] username;
  char[256] email;

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
  foreach (F; T.init.tupleof)
    n += F.sizeof;
  return n;
}();

@("Row test")
unittest {
  assert(Row.id.sizeof == 4);
  assert(Row.username.sizeof == 33);
  assert(Row.email.sizeof == 256);
  assert(Row.id.offsetof == 0);
  assert(Row.username.offsetof == 4);
  assert(Row.email.offsetof == 4 + 33);
  static assert(TotalFieldSizeOf!Row == 4 + 33 + 256);

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
enum uint TABLE_MAX_PAGES = 100;
enum uint ROW_SIZE = TotalFieldSizeOf!Row;

enum ExecuteResult {
  SUCCESS,
  TABLE_FULL
}

struct Pager {
  FILE* file;
  uint file_length;
  uint num_pages;
  void*[TABLE_MAX_PAGES] pages;

  void open(const(char)* filename) {
    this.file = fopen(filename, "ab+");
    if (this.file is null) {
      printf("Unable to open file\n");
      exit(EXIT_FAILURE);
    }
    fseek(this.file, 0, SEEK_END);
    this.file_length = cast(uint) ftell(this.file);
    fseek(this.file, 0, SEEK_SET);
    this.num_pages = this.file_length / PAGE_SIZE;
    if (this.file_length % PAGE_SIZE != 0) {
      printf("Db file is not a whole number of pages. Corrupt file.\n");
      exit(EXIT_FAILURE);
    }
  }

  void close() {
    foreach (i; 0 .. this.num_pages) {
      if (this.pages[i] is null) {
        continue;
      }
      this.flush(i);
      free(this.pages[i]);
      this.pages[i] = null;
    }

    if (fclose(this.file) != 0) {
      printf("Error closing db file.\n");
      exit(EXIT_FAILURE);
    }
    foreach (ref p; this.pages) {
      if (p) {
        free(p);
        p = null;
      }
    }
  }

  void flush(uint page_num) {
    if (this.pages[page_num] is null) {
      printf("Tried to flush null page\n");
      exit(EXIT_FAILURE);
    }

    if (fseek(this.file, page_num * PAGE_SIZE, SEEK_SET) != 0) {
      printf("Error seeking: %d\n", errno);
      exit(EXIT_FAILURE);
    }
    if (fwrite(this.pages[page_num], PAGE_SIZE, 1, this.file) != 1) {
      printf("Error writing: %d\n", errno);
      exit(EXIT_FAILURE);
    }
  }

  LeafNode* get_page(uint page_num) {
    if (page_num > this.pages.length) {
      printf("Tried to fetch page number out of bounds. %d > %ld\n",
        page_num,
        this.pages.length);
      exit(EXIT_FAILURE);
    }
    if (this.pages[page_num] is null) {
      // Cache miss. Allocate memory only when we try to access page.
      void* page = this.pages[page_num] = malloc(PAGE_SIZE);
      auto num_pages = this.file_length / PAGE_SIZE;
      // We might save a partial page at the EOF.
      if (this.file_length % PAGE_SIZE) {
        ++num_pages;
      }
      if (page_num <= num_pages && this.file !is null) {
        fseek(this.file, page_num * PAGE_SIZE, SEEK_SET);
        fread(page, PAGE_SIZE, 1, this.file);
        if (ferror(this.file) != 0) {
          printf("Error reading file: %d\n", errno);
          exit(EXIT_FAILURE);
        }
      }
      this.pages[page_num] = page;
      if (page_num >= this.num_pages) {
        this.num_pages = page_num + 1;
      }
    }
    return cast(LeafNode*) this.pages[page_num];
  }
}

struct Cursor {
  Table* table;
  uint page_num, cell_num;

  // InputRange requires these three.
  bool empty() {
    return this.cell_num >= this.node.num_cells;
  }

  void popFront() {
    ++this.cell_num;
  }

  LeafNode* node() {
    return this.table.pager.get_page(this.page_num);
  }

  void* front() {
    return this.node.cells[this.cell_num].value.ptr;
  }

  void insert(uint key, const ref Row value) {
    if (this.node.num_cells >= LeafNode.MAX_CELLS) {
      // Node full
      printf("Need to implement splitting a leaf node.\n");
      exit(EXIT_FAILURE);
    }

    if (this.cell_num < this.node.num_cells) {
      // Make room for new cell
      for (uint i = this.node.num_cells; i < this.cell_num; --i) {
        // this.node.cells[i] = this.node.cells[i - 1];
        memcpy(&this.node.cells[i], &this.node.cells[i - 1], LeafNode.Cell.sizeof);
      }
    }
    ++this.node.num_cells;
    this.node.cells[this.cell_num].key = key;
    value.serialize(&this.node.cells[this.cell_num].value);
  }
}

struct Table {
  uint root_page_num;
  Pager pager;

  void open(char* filename) {
    this.pager.open(filename);
    if (this.pager.num_pages == 0) {
      // New database file.
      auto root_node = this.pager.get_page(0);
      *root_node = LeafNode();
    }
  }

  ~this() {
    foreach (p; this.pager.pages) {
      if (p is null)
        break;
      free(p);
    }
  }

  Cursor start() {
    return Cursor(&this, this.root_page_num);

  }

  Cursor end() {
    return Cursor(&this,
      this.root_page_num,
      this.pager.get_page(this.root_page_num).num_cells);
  }

  ExecuteResult insert(ref const Row row) {
    if (this.pager.get_page(this.root_page_num).num_cells >= LeafNode.MAX_CELLS) {
      return ExecuteResult.TABLE_FULL;
    }
    this.end().insert(row.id, row);
    return ExecuteResult.SUCCESS;
  }

  ExecuteResult select() {
    Row row;
    foreach (void* x; this.start()) {
      row.deserialize(x);
      row.print();
    }
    return ExecuteResult.SUCCESS;
  }
}

// @("Table test")
// unittest {
//   Table table;
//   Row row;
//   foreach (_; 0 .. TABLE_MAX_ROWS) {
//     assert(table.insert(row) == ExecuteResult.SUCCESS);
//   }
//   assert(table.insert(row) == ExecuteResult.TABLE_FULL);
// }

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
    if (id < 0)
      return PrepareResult.NEGATIVE_ID;
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
  long_query[$ - 1] = 0;
  assert(statement.prepare(long_query) == PrepareResult.STRING_TOO_LONG);
}

void print_constants() {
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", LeafNode.Header.sizeof);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LeafNode.Cell.sizeof);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LeafNode.SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %d\n", LeafNode.MAX_CELLS);
}

void print_leaf_node(LeafNode* node) {
  printf("leaf (size %d)\n", node.num_cells);
  for (uint i = 0; i < node.num_cells; i++) {
    printf("  - %d : %d\n", i, node.cells[i].key);
  }
}

MetaCommandResult do_meta_command(char[] buffer, ref Table table) {
  if (strcmp(buffer.ptr, ".exit") == 0) {
    free(buffer.ptr);
    table.pager.close();
    exit(EXIT_SUCCESS);
  }
  else if (strcmp(buffer.ptr, ".btree") == 0) {
    printf("Tree:\n");
    print_leaf_node(table.pager.get_page(0));
    return MetaCommandResult.SUCCESS;
  }
  else if (strcmp(buffer.ptr, ".constants") == 0) {
    printf("Constants:\n");
    print_constants();
    return MetaCommandResult.SUCCESS;
  }
  else {
    return MetaCommandResult.UNRECOGNIZED;
  }
}

void print_prompt() {
  printf("db > ");
}

char[] read_stdin() {
  int len = 1024;
  char* ptr = cast(char*) malloc(len);
  if (fgets(ptr, len, stdin) is null) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }
  // Ignore trailing new line;
  size_t slen = strlen(ptr);
  ptr[slen - 1] = 0;
  return ptr[0 .. slen];
}

@("Assert true test")
unittest {
  assert(true, "this should pass.");
}

version (unittest) {
  enum RESET = "\033[0m";
  enum RED = "\033[31m";
  enum GREEN = "\033[32m";

  private template from(string modname) {
    mixin("import from = ", modname, ";");
  }

  extern (C) int main() {
    import core.stdc.stdio;
    import std.traits;

    static foreach (mod; ["app"]) {
      static foreach (test; __traits(getUnitTests, from!mod)) {
        {
          auto udas = getUDAs!(test, string);
          static if (udas.length) {
            printf("unittest: [%s] %s ... ", mod.ptr, udas[0].ptr);
          }
          else {
            printf("unittest: [%s] %s ... ", mod.ptr, test.stringof.ptr);
          }
          test();
          printf("OK\n");
        }
      }
    }
    printf(GREEN ~ "=== UNIT TEST PASSED 😉 ===\n" ~ RESET);
    return 0;
  }
}
else {
  extern (C) int main(int argc, char** argv) {
    if (argc < 2) {
      printf("Must supply a database filename.\n");
      exit(EXIT_FAILURE);
    }
    char* filename = argv[1];
    Table table;
    table.open(filename);

    while (true) {
      print_prompt();
      char[] input_buffer = read_stdin();
      scope (exit)
        free(input_buffer.ptr);

      if (input_buffer[0] == '.') {
        final switch (do_meta_command(input_buffer, table)) {
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
  }
}
