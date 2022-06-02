function sumArr(arr) {
	var res = 0
	for (var i = 0; i < arr.length; i++) {
		res += arr[i]
	}

	return res
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

