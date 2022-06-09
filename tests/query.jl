sums = [0.0, 0.0, 0.0, 0.0, 0.0]
total = 0
function scan(
	open::Vector{Float32},
	high::Vector{Float32},
	low::Vector{Float32},
	close::Vector{Float32},
	volume::Vector{UInt64}
)
	global total += size(close, 1)
	global sums[1] += sum(map((x) -> convert(Float64, x), open))
	global sums[2] += sum(map((x) -> convert(Float64, x), high))
	global sums[3] += sum(map((x) -> convert(Float64, x), low))
	global sums[4] += sum(map((x) -> convert(Float64, x), close))
	global sums[5] += sum(volume)
	(total, sums)
end
