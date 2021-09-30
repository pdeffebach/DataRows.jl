# Definition
struct DataRow <: Tables.AbstractRow
	d::OrderedDict{Symbol, V} where V

	DataRow(d::OrderedDict{Symbol, V}) where V = new(d)
end

# Constructors
function DataRow(x)
	d = OrderedDict(x)
	if d isa OrderedDict{Symbol}
		DataRow(d)
	elseif d isa OrderedDict{<:AbstractString}
		z = zip(Symbol.(keys(d)), values(d))
		DataRow(OrderedDict(z))
	else
		throw(ArgumentError("Keys must be `Symbol`s or `AstractString`s"))
	end
end

function DataRow(;kwargs...)
	if isempty(kwargs)
		DataRow(OrderedDict{Symbol, Any}())
	else
		DataRow(OrderedDict(pairs(kwargs)))
	end
end

DataRow(n::NamedTuple) = DataRow(OrderedDict(pairs(n)))

function DataRow(names::AbstractVector{<:AbstractString}, values::AbstractVector)
	z = zip(Symbol.(names), values)
	DataRow(OrderedDict(z))
end
DataRow(names::AbstractVector{Symbol}, values::AbstractVector) = DataRow(OrderedDict(zip(names, values)))

# Getindex
innerdict(r::DataRow) = getfield(r, :d)

index(r, k::Symbol) = haskey(r, k) ? findfirst(==(k), propertynames(r)) : throw(KeyError(k))
index(r, k::AbstractString) = haskey(r, k) ? findfirst(==(k), names(r)) : throw(KeyError(k))
index(r, k::Int) = haskey(r, k) ? findfirst(==(k), 1:length(r)) : throw(KeyError(k))

Base.IndexStyle(::Type{DataRow}) = Base.IndexLinear()

Base.getindex(r::DataRow, k::AbstractString) =
	getindex(innerdict(r), Symbol(k))

Base.getindex(r::DataRow, k::Symbol) =
	getindex(innerdict(r), k)

function Base.getindex(r::DataRow, k::Int)
	d = innerdict(r)
	n = d.vals[k]
end

Base.getindex(r::DataRow, ks::AbstractVector{<:Union{Symbol, <:AbstractString}}) =
	DataRow(ks, [r[k] for k in ks])

Base.getindex(r::DataRow, ks::AbstractVector{Int}) =
	DataRow(propertynames(r)[ks], [r[k] for k in ks])

Base.getindex(r::DataRow, k::InvertedIndex{<:Union{AbstractString, Symbol, Int}}) =
	getindex(r, setdiff(1:length(r), index(r, k.skip)))

Base.getindex(r::DataRow, ks::InvertedIndices.InvertedIndex{<:AbstractVector{<:Union{AbstractString, Symbol, Int}}}) =
	getindex(r, setdiff(1:length(r), [index(r, ki) for ki in ks.skip]))

function Base.getindex(r::DataRow, ks::Between{T, T}) where T
	f = index(r, ks.first)
	l = index(r, ks.last)

	mf, ml = min(f, l), max(f, l)
	r[mf:ml]
end

Base.getindex(r::DataRow, ::Colon) = copy(r)

function Base.getindex(r::DataRow, ks::Cols)
	inds = Int[]
	for k in ks.cols
		if k isa AbstractVector
			append!(inds, index.(Ref(r), k))
		else
			append!(inds, index(r, k))
		end
	end
	r[unique(inds)]
end

# Push
function Base.push!(r::DataRow, kv::Pair{<:AbstractString})
	push!(innerdict(r), Symbol(first(kv)) => last(kv))
	r
end

function Base.push!(r::DataRow, kv::Pair{Symbol})
	push!(innerdict(r), kv)
	r
end

# Setindex
Base.setindex!(r::DataRow, v::Any, k::AbstractString) =
	setindex!(innerdict(r), v::Any, Symbol(k))

Base.setindex!(r::DataRow, v::Any, k::Symbol) =
	setindex!(innerdict(r), v, k)

function Base.setindex!(r::DataRow, v, k::Int)
	setindex!(innerdict(r).vals, v, k)
end

# Getproperty
Base.getproperty(r::DataRow, k::AbstractString) =
	getindex(innerdict(r), Symbol(k))

Base.getproperty(r::DataRow, k::Symbol) =
	getindex(innerdict(r), k)

function Base.getproperty(r::DataRow, k::Int)
	d = innerdict(r)
	n = d.vals[k]
end

# Setproperty
Base.setproperty!(r::DataRow, k::AbstractString, v) =
	setindex!(innerdict(r), v, k)

Base.setproperty!(r::DataRow, k::Symbol, v) =
	setindex!(innerdict(r), v, k)

# Tables.jl interface
Tables.getcolumn(r::DataRow, i::Int) = r[i]
Tables.getcolumn(r::DataRow, nm::Symbol) = r[nm]
Tables.columnnames(r::DataRow) = propertynames(r)
function Tables.getcolumn(r::DataRow, ::Type{T}, i::Int, nm::Symbol)::AbstractVector{T} where {T}
	propertynames(r)[i] != nm && throw(ArgumentError("Name $nm does not match index $i"))
	getcolumn(r, nm)
end

function Tables.columntable(r::DataRow)
	pnames = propertynames(r)
	vals = values(r)
	N = length(r)
	nms = ntuple(i -> pnames[i], N)
	vals = ntuple(i -> [vals[i]], N)
	NamedTuple{nms}(vals)
end

# Copy
Base.copy(r::DataRow) = DataRow(pairs(r))

# Names
Base.names(r::DataRow) = string.(keys(innerdict(r)))
Base.propertynames(r::DataRow; private::Bool = false) = innerdict(r).keys

# Keys
Base.keys(r::DataRow) = innerdict(r).keys

Base.haskey(r::DataRow, k::Bool) =
    throw(ArgumentError("invalid key: $key of type Bool"))
Base.haskey(r::DataRow, k::Int) = 1 <= key <= size(r, 1)
Base.haskey(r::DataRow, k::AbstractString) = haskey(innerdict(r), Symbol(k))
Base.haskey(r::DataRow, k::Symbol) = haskey(innerdict(r), k)

Base.get(r::DataRow, k::AbstractString, default) =
    haskey(r, k) ? r[k] : default

Base.get(r::DataRow, k::Symbol, default) =
    haskey(r, k) ? r[k] : default

    Base.get(r::DataRow, k::Int, default) =
    haskey(r, k) ? r[k] : default

Base.get(f::Base.Callable, r::DataRow, k::AbstractString) =
    haskey(r, k) ? dfr[k] : f()

Base.get(f::Base.Callable, r::DataRow, k::Symbol) =
    haskey(r, k) ? dfr[k] : f()

Base.get(f::Base.Callable, r::DataRow, k::Int) =
    haskey(r, k) ? dfr[k] : f()

Base.values(r::DataRow) = innerdict(r).vals

# Iteration
Base.IteratorSize(::Type{DataRow}) =  Base.HasShape{1}()
Base.IteratorEltype(::Type{DataRow}) = Base.EltypeUnknown()

Base.iterate(r::DataRow) = iterate(values(innerdict(r)), 1)

Base.iterate(r::DataRow, st) = iterate(values(innerdict(r)), st)

# Size and length
Base.ndims(::DataRow) = 1
Base.ndims(::Type{DataRow}) = 1

Base.length(r::DataRow) = length(innerdict(r).vals)
Base.firstindex(r::DataRow) = 1
Base.lastindex(r::DataRow) = length(innerdict(r).vals)

Base.size(r::DataRow) = (length(r),)
Base.size(r::DataRow, i) = (size(r)[i],)

Base.isempty(r::DataRow) = length(r) == 0

# Broadcast and map
Base.BroadcastStyle(::Type{<:DataRow}) = Broadcast.Style{DataRow}()

function Base.copyto!(r::DataRow, bc::Broadcasted)
	bcf = Broadcast.flatten(bc)
	bcf2 = Base.Broadcast.preprocess(r, bcf)

	for i in 1:length(r)
		r[i] = bcf[i]
	end
	r
end

function Base.map(f, r::DataRow, rs::DataRow...)
	n = propertynames(r)
	if !all(t -> propertynames(t) == n, rs)
		throw(ArgumentError("DataRow names do not match"))
	end
	valsout = map(f, copy(values(r)), copy.(values.(rs))...)
	DataRow(names(r), valsout)
end

function Base.map!(f, r::DataRow, rs::DataRow...)
	n = propertynames(r)
	if !all(t -> propertynames(t) == n, rs)
		throw(ArgumentError("DataRow names do not match"))
	end
	valsout = map!(f, values(r), copy.(values.(rs))...)
	DataRow(names(r), valsout)
end

# Conversions
function Base.convert(::Type{Vector}, r::DataRow)
    T = mapreduce(typeof, promote_type, r)
    convert(Vector{T}, r)
end

Base.convert(::Type{Vector{T}}, r::DataRow) where T =
	T[ri for ri in r]
Base.Vector(r::DataRow) = convert(Vector, r)
Base.Vector{T}(r::DataRow) where T = convert(Vector{T}, r)

Base.convert(::Type{Array}, r::DataRow) = Vector(r)
Base.convert(::Type{Array{T}}, r::DataRow) where {T} = Vector{T}(r)
Base.Array(r::DataRow) = Vector(r)
Base.Array{T}(r::DataRow) where {T} = Vector{T}(r)

OrderedCollections.OrderedDict(r::DataRow) = copy(innerdict(r))

Base.Dict(r::DataRow) = Dict(innerdict(r))

function Base.NamedTuple(r::DataRow)
	vals = Tuple(values(r))
	nms = Tuple(propertynames(r))
	NamedTuple{nms}(vals)
end

# Merging
Base.merge(r::DataRow) = copy(r)
Base.merge(r::DataRow, s::NamedTuple) = merge(r, DataRow(s))
Base.merge(r::NamedTuple, s::DataRow) = merge(DataRow(r), s)
Base.merge(r::DataRow, s::DataRow) = DataRow(merge(innerdict(r), innerdict(s)))
Base.merge(r::DataRow, s::Base.Iterators.Pairs) = merge(r, DataRow(OrderedDict(s)))

# Equality
function Base.:(==)(r::DataRow, s::DataRow)
	propertynames(r) == propertynames(s) || return false
	all(x -> first(x) == last(x), zip(r, s))
end

function Base.isequal(r::DataRow, s::DataRow)
	propertynames(r) == propertynames(s) || return false
	all(x -> isequal(first(x), last(x)), zip(r, s))
end

function Base.isless(r::DataRow, s::DataRow)
	length(r) == length(s) ||
		throw(ArgumentError("compared `DataRow`s must have same length " *
                          "(got $(length(r)) and $(length(s)))"))

	if propertynames(r) != propertynames(s)
		mismatch = findfirst(i -> names(r)[i] != names(s)[i], 1:length(r))
		throw(ArgumentError("Compared `DataRow`s must have same names. " *
			                "The mismatched names are $(names(r)[mismatch]) and " *
                            "$(names(s)[mismatch]) respectively"))
	end

    for (a, b) in zip(r, s)
        isequal(a, b) || return isless(a, b)
    end
    return false
end