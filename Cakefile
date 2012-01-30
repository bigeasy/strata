edify = require("./edify/lib/edify")()
edify.language "coffee"
  lexer: "coffeescript"
  docco: "#"
  ignore: [ /^#!/, /^#\s+vim/ ]
edify.language "c"
  lexer: "c"
  ignore: [ /^#!/, /^# vim/ ]
  docco:
    start:  /^\s*\s(.*)/
    end:    /^(.*)\*\//
    strip:  /^\s+\*/
edify.parse "coffee", "code/src/lib", ".", /\.coffee$/
edify.stencil /\/.*.coffee$/, "stencil/docco.stencil"
edify.tasks task
