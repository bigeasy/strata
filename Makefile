all: index.html

index.html: code/src/lib/strata._coffee
	stencil "stencil//docco.stencil" "./index.html" "code/src/lib/strata._coffee"
