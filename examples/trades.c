#include "../lib/tickdb.h"
#include "../lib/inttypes.h"

#include <stdlib.h>

typedef struct trade {
  i64 ts;
  char* sym;
  i64 ts_participant;
  i64 ts_trf;
  u32 size;
  float price;
  u32 conditions;
  u8 error;
  u8 exchange;
  u8 tape;
  u64 id;
  u64 seq_id;
} trade;

i64 ts = 0;

void generate_trade(trade* trade) {
  trade->ts = ts++;
  trade->sym = "000";
  trade->sym[0] = (rand() % 26);
  trade->sym[1] = (rand() % 26);
  trade->sym[2] = (rand() % 26);
  trade->ts_participant = ts++;
  trade->ts_trf = ts++;
}

int main(void) {
  tickdb_schema s = tickdb_schema_init("trades", "%Y-%m-%d", TICKDB_SYMBOL16, "us_equities");
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
  
  int num_trades = 1000000000;
  trade t;
  for (int i = 0; i < num_trades; i++) {
    generate_trade(&t);
    tickdb_table_write(&trades, t.ts, t.sym);
    tickdb_table_write_int64(&trades, t.ts_participant);
    tickdb_table_write_uint64(&trades, t.id);
    tickdb_table_write_uint64(&trades, t.seq_id);
    tickdb_table_write_uint32(&trades, t.size);
    tickdb_table_write_double(&trades, t.price);
    tickdb_table_write_uint32(&trades, t.conditions);
    tickdb_table_write_uint8(&trades, t.error);
    tickdb_table_write_uint8(&trades, t.exchange);
    tickdb_table_write_uint8(&trades, t.tape);
  }

  return 0;
}
