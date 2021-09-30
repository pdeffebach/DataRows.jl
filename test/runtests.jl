module TestDataRows

using Test, DataRows, OrderedCollections, Statistics

const ≅ = isequal

@testset "constructors" begin
	d = OrderedDict([:a => 1, :b => 2])
	r = DataRow(d)

	@test DataRow() == DataRow(OrderedDict{Symbol, Any}())

	@test DataRow(pairs(d)) == r
	@test DataRow(a = 1, b = 2) == r
	@test DataRow([:a, :b], [1, 2]) == r
	@test DataRow(["a", "b"], [1, 2]) == r
	@test DataRow([:a => 1, :b => 2]) == r
	@test DataRow(["a" => 1, "b" => 2]) == r
	@test DataRow((a = 1, b = 2)) == r
end

@testset "getindex" begin
	r = DataRow(a = 1, b = 2)

	@test r[:a] == 1
	@test r["a"] == 1
	@test r[[:a, :b]] == r
	@test r[["a", "b"]] == r
	@test r[[:b, :a]] == DataRow(b = 2, a = 1)

	@test r[Not(:a)] == DataRow(b = 2)
	@test r[Between(:a, :b)] == r
	@test r[Between(:b, :a)] == r

	@test r[Cols(:a, [:a, :b])] == r

	@test r[:] == r
end

@testset "push!" begin
	r = DataRow(a = 1, b = 2)
	rn = DataRow(a = 1, b = 2, c = 3)

	@test push!(r, :c => 3) == rn
	@test push!(r, "c" => 3) == rn
end

@testset "setindex!" begin
	r = DataRow(a = 1, b = 2)

	r[1] = 5
	@test r == DataRow(a = 5, b = 2)
	r[:a] = 6
	@test r == DataRow(a = 6, b = 2)
	r["a"] = 7
	@test r == DataRow(a = 7, b = 2)
	r.a = 8
	@test r == DataRow(a = 8, b = 2)
	r."a" = 9
	@test r == DataRow(a = 9, b = 2)
end

end