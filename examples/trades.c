#include "../src/tickdb.h"
#include "../src/util/inttypes.h"

#include <signal.h>
#include <stdlib.h>

static volatile sig_atomic_t keep_running = 1;

static void sig_handler(int _) {
	(void)_;
	keep_running = 0;
}

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
	// trade->sym[2] = 'A' + (rand() % 26);
	trade->sym[2] = '\0';
	trade->ts_participant = ts++;
	trade->ts_trf = ts++;
}

int main(void) {
	signal(SIGINT, sig_handler);

	tdb_table* trades = tdb_table_open("trades");
	if (trades == NULL) {
		printf("making new table\n");
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
		trades = tdb_table_init(s);
	}
	if (trades == NULL) {
		exit(1);
	}

	int num_trades = 10000000;
	trade t;
	srand(0);
	for (int i = 1; keep_running && i <= num_trades; i++) {
		generate_trade(&t);
		// printf("i %d\n", i);
		if (i % 1000000 == 0)
			printf("%d %s\n", i, t.sym);
		if (tdb_table_write(trades, t.sym, t.ts))
			exit(1);
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

	tdb_table_flush(trades);

	printf("reading\n");
	u32 count = 0;
	f64 sum_price = 0.0;
	const char* cols[] = { "price" };
	tdb_iter* iter = tdb_table_iter(trades, NULL, 0, 1000, cols);
	printf("iter len %lu\n", iter->len);
	while (tdb_iter_next(iter)) {
		tdb_arr_f64* price = tdb_iter_next_f64(iter);
		count += price->len;
		for (int j = 0; j < price->len; j++)
			sum_price += price->data[j];
	}
	printf("count %u sum_price %f\n", count, sum_price);
	
	tdb_iter_free(iter);
	if (tdb_table_close(trades))
		exit(1);

	return 0;
}

