function sumArr(arr) {
	return arr.reduce((acc, cur) => acc + cur, 0);
}

var sum = 0
function scan(open) {
	//sum += sumArr(open);
	for (var i = 0; i < 12000; i++) {
		sum += open[i];
	}
	return sum;
}

//function scan(open, high, low, close, volume) {
//	console.log('scan run', open.legnth, high.length, low.length, close.length, volume.length);
//	sum += sumArr(open) + sumArr(high) + sumArr(low) + sumArr(close) + sumArr(volume);
//
//	return sum;
//}

