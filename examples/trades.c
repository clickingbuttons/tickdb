#include "../lib/tickdb.h"
#include "../lib/inttypes.h"

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
  tickdb_schema s = tickdb_schema_init("trades", "%Y/%m/%d", TICKDB_SYMBOL16, "us_equities");
  tickdb_schema_add(&s, "ts_participant", TICKDB_TIMESTAMP);
  tickdb_schema_add(&s, "id", TICKDB_UINT64);
  tickdb_schema_add(&s, "seq_id", TICKDB_UINT64);
  tickdb_schema_add(&s, "size", TICKDB_UINT32);
  tickdb_schema_add(&s, "price", TICKDB_CURRENCY);
  tickdb_schema_add(&s, "cond", TICKDB_UINT32);
  tickdb_schema_add(&s, "err", TICKDB_UINT8);
  tickdb_schema_add(&s, "exchange", TICKDB_UINT8);
  tickdb_schema_add(&s, "tape", TICKDB_UINT8);

  tickdb_table trades = tickdb_table_init(&s);
  
  int num_trades = 10000000;
  trade t;
  for (int i = 0; i < num_trades; i++) {
    if (i % 1000000 == 0) {
      printf("%d %s\n", i, t.sym);
    }
    generate_trade(&t);
    tickdb_table_write(&trades, t.sym, t.ts);
    tickdb_table_write_i64(&trades, t.ts_participant);
    tickdb_table_write_u64(&trades, t.id);
    tickdb_table_write_u64(&trades, t.seq_id);
    tickdb_table_write_u32(&trades, t.size);
    tickdb_table_write_f64(&trades, t.price);
    tickdb_table_write_u32(&trades, t.conditions);
    tickdb_table_write_u8(&trades, t.error);
    tickdb_table_write_u8(&trades, t.exchange);
    tickdb_table_write_u8(&trades, t.tape);
  }

  return 0;
}

