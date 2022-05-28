function sumArr(arr) {
	return arr.reduce((acc, cur) => acc + Number(cur), 0);
}

var sum = 0
function scan(volume) {
	sum += sumArr(volume);

	return sum;
}

