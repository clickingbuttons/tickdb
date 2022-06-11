using HTTP
using JSON

fname = string(@__DIR__) * "/query.jl"
f = open(fname, "r")
query = read(f, String)
close(f)

post_data = Dict(
	"table" => "agg1d0",
	"from" => "1970-01-01",
	"to" =>   "1970-01-02",
	"source" => Dict(
		"text" => query,
		"path" => fname
  )
)
post_data = JSON.json(post_data)

headers = Dict(
	"Content-Type" => "application/json",
)

resp = HTTP.request("POST", "http://localhost:8080", [], post_data)

println(resp)
