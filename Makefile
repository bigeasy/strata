all: test/readme.t.js README.md

test/readme.t.js: edify.md
	edify --mode code $< > $@
README.md: edify.md
	edify --mode text $< > $@
