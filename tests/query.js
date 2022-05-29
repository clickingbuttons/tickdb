function sumArr(arr) {
	return arr.reduce((acc, cur) => acc + Number(cur), 0);
}

var sum = 0
function scan(open, high, low, close, volume) {
	sum += sumArr(open);
	sum += sumArr(high);
	sum += sumArr(low);
	sum += sumArr(close);
	sum += volume.length;

	return sum;
}

