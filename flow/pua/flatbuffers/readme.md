This directory is licensed under Apache V2 the files are a derivative work of https://github.com/google/flatbuffers/tree/master/lua

This Go port exists for 3 reasons:
1. upstream lua generator assumes 5.3 (relies on string.unpack/string.pack/integer) despite compatibiltiy shims. gopher-lua is 5.1+goto. So need the code to work with 64 bit integers while Lua 5.1 numbers are all float64
2. their lua runtime library has some flaws, like encoding bool as ascii "0" or "1" instead of "\0" or "\1"
3. last & definitely least, perf
