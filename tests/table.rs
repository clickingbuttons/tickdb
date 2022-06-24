use tickdb::{schema::*, table::*};

static ROW_COUNT: usize = 10_000_000;
static FROM_TS: i64 = 0;
static TO_TS: i64 = 365 * 24 * 60 * 60 * 1_000_000_000;

static ALPHABET: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

#[derive(Debug, Clone)]
struct OHLCV {
	ts:       i64,
	symbol:   String,
	open:     f32,
	high:     f32,
	low:      f32,
	close:    f32,
	close_un: f32,
	volume:   u32
}

fn generate_symbol(num_chars: usize, rng: &fastrand::Rng) -> String {
	let mut res = String::with_capacity(num_chars);
	for _ in 0..num_chars {
		let rand_index = rng.usize(..ALPHABET.len());
		res += &ALPHABET[rand_index..rand_index + 1];
	}

	res
}

fn generate_row(ts: i64, rng: &fastrand::Rng) -> OHLCV {
	OHLCV {
		ts,
		symbol: generate_symbol(rng.usize(1..5), rng),
		open: rng.f32(),
		high: rng.f32(),
		low: rng.f32(),
		close: rng.f32(),
		close_un: rng.f32(),
		volume: rng.u32(0..1000)
	}
}

fn generate_rows(row_count: usize, rng: &fastrand::Rng) -> Vec<OHLCV> {
	let mut res = Vec::with_capacity(row_count);

	for i in 0..row_count {
		let row = generate_row(i as i64, rng);
		res.push(row);
	}

	res
}

fn initialize_agg1d(index: i64) -> Table {
	let schema = Schema::new(&format!("agg1d{}", index), "%Y").add_cols(vec![
		Column::new("sym", ColumnType::Symbol),
		Column::new("open", ColumnType::F32),
		Column::new("high", ColumnType::F32),
		Column::new("low", ColumnType::F32),
		Column::new("close", ColumnType::F32),
		Column::new("close_un", ColumnType::F32),
		Column::new("volume", ColumnType::U64),
	]);

	let mut agg1d = Table::create(schema).expect("Could not open table");
	let row_count = ROW_COUNT;
	let rows = generate_rows(row_count, &fastrand::Rng::with_seed(100));
	for r in rows.iter() {
		let ts = match agg1d.get_last_ts() {
			Some(ts) => ts,
			None => 0
		};
		agg1d.put(ts + r.ts);
		agg1d.put_symbol(&r.symbol);
		agg1d.put_f32(r.open);
		agg1d.put_f32(r.high);
		agg1d.put_f32(r.low);
		agg1d.put_f32(r.close);
		agg1d.put_f32(r.close_un);
		agg1d.put_u32(r.volume);
	}
	agg1d.flush();

	agg1d
}

fn get_f64_sum(slice: &[f32]) -> f64 {
	slice.iter().map(|v| *v as f64).sum::<f64>()
}

#[test]
fn sum_ohlcv_rust() {
	let table = initialize_agg1d(0);

	let mut sums = (0 as u64, 0 as u64, 0.0, 0.0, 0.0, 0.0, 0 as u64);
	let mut total = 0;
	let partitions = table.partition_iter(FROM_TS, TO_TS, vec![
		"ts", "sym", "open", "high", "low", "close", "volume",
	]).unwrap();
	for partition in partitions {
		sums.0 += partition[0].get_u64().iter().fold(0, |acc, ts| acc ^ ts);
		sums.1 += partition[1]
			.get_sym()
			.iter()
			.map(|s| s.len() as u64)
			.sum::<u64>();
		sums.2 += get_f64_sum(partition[2].get_f32());
		sums.3 += get_f64_sum(partition[3].get_f32());
		sums.4 += get_f64_sum(partition[4].get_f32());
		sums.5 += get_f64_sum(partition[5].get_f32());
		sums.6 += partition[6]
			.get_u32()
			.iter()
			.map(|v| *v as u64)
			.sum::<u64>();
		total += partition[6].get_u64().iter().len();
	}
	assert_eq!(sums.0, 9182783923072);
	assert_eq!(sums.1, 24995152);
	assert_eq!(sums.2, 5000173.621421814);
	assert_eq!(sums.3, 5001427.372336388);
	assert_eq!(sums.4, 4999160.529966474);
	assert_eq!(sums.5, 4999284.882663012);
	assert_eq!(sums.6, 4994871927);
	assert_eq!(total, ROW_COUNT);
}
