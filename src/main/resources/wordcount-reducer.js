fetch(function(data) {
	var results = [];
	var first = data.value[0];
	var count = first.count;
	var word = first.word
	for (var i = 1; i < data.value.length; i++) {
		var value = data.value[i];
		if (value.word == word) {
			count += value.count;
		}
		else {
			results.push(new WordCount(word, count));
			word = value.word;
			count = value.count;
		}
		
		if (i + 1 == data.value.length) {
			results.push(new WordCount(word, count));
		}
	}
	push(new WordCountList(data.id, results));
});

function WordCount(word, count) {
	this.word = word;
	this.count = count;
}

function WordCountList(id, wordCounts) {
	this.id = id;
	this.wordCounts = wordCounts;
}