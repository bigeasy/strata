{Twinkie}       = require "./vendor/twinkie/lib/twinkie"

twinkie = new Twinkie
twinkie.ignore "docs", "t/*/tmp"
twinkie.master "javascript"
twinkie.coffee  "src/lib", "lib"
twinkie.copy    "src/lib", "lib", /\.js$/
twinkie.tasks task, "compile", "idl", "docco", "gitignore"
