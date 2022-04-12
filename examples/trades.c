#include "../lib/inttypes.h"
#include "../lib/tickdb.h"

#include <stdio.h>
#include <stdlib.h>

typedef struct trade {
  int64_t ts;
  char sym[4];
  int64_t ts_participant;
  int64_t ts_trf;
  uint32_t size;
  float price;
  uint32_t conditions;
  uint8_t error;
  uint8_t exchange;
  uint8_t tape;
  uint64_t id;
  uint64_t seq_id;
} trade;

int64_t ts = 0;

void generate_trade(trade* trade) {
  trade->ts = ts++;
  // 26**3 = 17576
  // log2(26**3) = 14.101319154423276
  trade->sym[0] = 'A' + (rand() % 26);
  trade->sym[1] = 'A' + (rand() % 26);
  trade->sym[2] = 'A' + (rand() % 26);
  trade->sym[3] = '\0';
  trade->ts_participant = ts++;
  trade->ts_trf = ts++;
}

int main(void) {
  tdb_schema* s =
   tdb_schema_init("trades", "%Y/%m/%d", TDB_SYMBOL16, "us_equities");
  tdb_schema_add(s, TDB_TIMESTAMP64, "ts_participant");
  tdb_schema_add(s, TDB_UINT64, "id");
  tdb_schema_add(s, TDB_UINT64, "seq_id");
  tdb_schema_add(s, TDB_UINT32, "size");
  tdb_schema_add(s, TDB_CURRENCY, "price");
  tdb_schema_add(s, TDB_UINT32, "cond");
  tdb_schema_add(s, TDB_UINT8, "err");
  tdb_schema_add(s, TDB_UINT8, "exchange");
  tdb_schema_add(s, TDB_UINT8, "tape");

  tdb_table* trades = tdb_table_init(s);

  int num_trades = 10000000;
  trade t;
  for (int i = 0; i < num_trades; i++) {
    generate_trade(&t);
    if (i % 1000000 == 0)
      printf("%d %s\n", i, t.sym);
    tdb_table_write(trades, t.sym, t.ts);
    tdb_table_write_i64(trades, t.ts_participant);
    tdb_table_write_u64(trades, t.id);
    tdb_table_write_u64(trades, t.seq_id);
    tdb_table_write_u32(trades, t.size);
    tdb_table_write_f64(trades, t.price);
    tdb_table_write_u32(trades, t.conditions);
    tdb_table_write_u8(trades, t.error);
    tdb_table_write_u8(trades, t.exchange);
    tdb_table_write_u8(trades, t.tape);
  }

  return 0;
}
