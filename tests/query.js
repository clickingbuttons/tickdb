function sumArr(arr) {
	return arr.reduce((acc, cur) => acc + cur, 0);
}

var sum = 0
function scan(open, high, low, close) {
	console.log('scan run', open.legnth, high.length, low.length, close.length);
	sum += sumArr(open) + sumArr(high) + sumArr(low) + sumArr(close)

	return sum;
}

