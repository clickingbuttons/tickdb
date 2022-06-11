total = Float32(0)
function scan(
	open::Vector{Float32},
	high::Vector{Float32},
	low::Vector{Float32},
	close::Vector{Float32},
	volume::Vector{UInt64}
)
	global total += sum(open)
	global total += sum(high)
	global total += sum(low)
	global total += sum(close)
	global total += sum(volume)

	total
end
