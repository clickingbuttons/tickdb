// https://www.geeksforgeeks.org/how-to-get-the-javascript-function-parameter-names-values-dynamically/
function tickdb_get_params(func) {
	// String representaation of the function code
	var str = func.toString();

	// Remove comments of the form /* ... */
	// Removing comments of the form //
	// Remove body of the function { ... }
	// removing '=>' if func is arrow function
	str = str.replace(/\/\*[\s\S]*?\*\//g, '')
					.replace(/\/\/(.)*/g, '')        
					.replace(/{[\s\S]*}/, '')
					.replace(/=>/g, '')
					.trim();

	// Start parameter names after first '('
	var start = str.indexOf("(") + 1;

	// End parameter names is just before last ')'
	var end = str.length - 1;

	var result = str.substring(start, end).split(", ");

	var params = [];

	result.forEach(element => {
		// Removing any default value
		element = element.replace(/=[\s\S]*/g, '').trim();

		if(element.length > 0)
			params.push(element);
	});
	 
	return params;
}
