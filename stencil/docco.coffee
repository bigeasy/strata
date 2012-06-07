module.exports =
  initializer: ->
    @edify = require("edify").create()
    @edify.language
      lexer: "coffeescript"
      docco: "#"
      ignore: [ /^#!/, /^#\s+vim/ ]
