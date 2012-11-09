var stopwords = ["a","able","about","across","after","all","almost","also","am","among","an","and","any","are","as","at","be","because","been","but","by","can","cannot","could","dear","did","do","does","either","else","ever","every","for","from","get","got","had","has","have","he","her","hers","him","his","how","however","i","if","in","into","is","it","its","just","least","let","like","likely","may","me","might","most","must","my","neither","no","nor","not","of","off","often","on","only","or","other","our","own","rather","said","say","says","she","should","since","so","some","than","that","the","their","them","then","there","these","they","this","tis","to","too","twas","us","wants","was","we","were","what","when","where","which","while","who","whom","why","will","with","would","yet","you","your"];

fetch(function(data) {
	var mapping = new Index();
	for (var i = 0; i < data.value.length; i++) {
		mapping = processLine(mapping, data.value[i]);
	}
	push(new Result(data.id, mapping.words));
});

function processLine(oldIndex, line) {
	return combine(oldIndex, getWords(line));
}

function getWords(line) {
	var index = new Index();
	var splits = line.split(" ");
	for (var i = 0; i < splits.length; i++) {
		var split = splits[i].replace(/[^0-9A-Za-z]/g, "").toLowerCase().trim();
		if(split.length > 0 && stopwords.indexOf(split) == -1)
		{
			index = add(index, new Count(split, 1));
		}
	}
	return index;
}

function Count(word, count) {
	this.word = word;
	this.count = count;
}

function Index() {
	this.words = [];
}

function combine(self, index) {
	for (var i = 0; i < index.words.length; i++) {
		self = add(self, index.words[i]);
	}
	return self;
}

function add(index, count) {
	var found = false;
	for (var i = 0; i < index.words.length; i++) {
		if (index.words[i].word == count.word) {
			index.words[i].count += count.count;
			found = true;
		}
	}
	if (!found) {
		index.words.push(count);
	}
	return index;
}

function Result(id, wordCounts) {
	this.id = id;
	this.wordCounts = wordCounts;
}
