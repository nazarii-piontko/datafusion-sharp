#pragma warning disable CA1010 // Collections should implement generic interface

using System.Collections;
using System.Data.Common;

namespace DataFusionSharp.Data;

/// <summary>
/// A typed, ordered collection of <see cref="DataFusionSharpParameter"/> objects for a <see cref="DataFusionSharpCommand"/>.
/// </summary>
public sealed class DataFusionSharpParameterCollection : DbParameterCollection
{
    private readonly List<DataFusionSharpParameter> _items = [];

    /// <inheritdoc />
    public override int Count => _items.Count;

    /// <inheritdoc />
    public override object SyncRoot => ((ICollection)_items).SyncRoot;

    /// <summary>
    /// Gets or sets the parameter at the specified index.
    /// </summary>
    public new DataFusionSharpParameter this[int index]
    {
        get => _items[index];
        set => _items[index] = value;
    }

    /// <summary>
    /// Gets or sets the parameter with the specified name (prefix-insensitive lookup).
    /// </summary>
    public new DataFusionSharpParameter this[string parameterName]
    {
        get => _items[IndexOfChecked(parameterName)];
        set => _items[IndexOfChecked(parameterName)] = value;
    }

    /// <summary>
    /// Adds the given parameter to the collection and returns it.
    /// </summary>
    public DataFusionSharpParameter Add(DataFusionSharpParameter parameter)
    {
        _items.Add(parameter);
        return parameter;
    }

    /// <summary>
    /// Creates a parameter with the given name and value, adds it to the collection, and returns it.
    /// </summary>
    public DataFusionSharpParameter AddWithValue(string parameterName, object? value)
    {
        var p = new DataFusionSharpParameter(parameterName, value);
        _items.Add(p);
        return p;
    }

    /// <inheritdoc />
    public override int Add(object value)
    {
        _items.Add(CastParameter(value));
        return _items.Count - 1;
    }

    /// <inheritdoc />
    public override void AddRange(Array values)
    {
        ArgumentNullException.ThrowIfNull(values);

        foreach (var v in values)
            Add(v!);
    }

    /// <inheritdoc />
    public override void Clear() => _items.Clear();

    /// <inheritdoc />
    public override bool Contains(object value) =>
        value is DataFusionSharpParameter p && _items.Contains(p);

    /// <inheritdoc />
    public override bool Contains(string value) => IndexOf(value) >= 0;

    /// <inheritdoc />
    public override void CopyTo(Array array, int index) =>
        ((ICollection)_items).CopyTo(array, index);

    /// <inheritdoc />
    public override IEnumerator GetEnumerator() => _items.GetEnumerator();

    /// <inheritdoc />
    public override int IndexOf(object value) =>
        value is DataFusionSharpParameter p ? _items.IndexOf(p) : -1;

    /// <summary>
    /// Returns the index of the parameter matching <paramref name="parameterName"/>.
    /// Name comparison is case-insensitive and strips any leading prefix characters (<c>@</c>, <c>$</c>, <c>:</c>).
    /// </summary>
    /// <returns>The index of the parameter with the given name, or -1 if not found.</returns>
    public override int IndexOf(string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(parameterName);
        
        for (var i = 0; i < _items.Count; i++)
        {
            if (string.Equals(_items[i].ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))
                return i;
        }
        return -1;
    }

    /// <inheritdoc />
    public override void Insert(int index, object value) =>
        _items.Insert(index, CastParameter(value));

    /// <inheritdoc />
    public override void Remove(object value)
    {
        if (value is DataFusionSharpParameter p)
            _items.Remove(p);
    }

    /// <inheritdoc />
    public override void RemoveAt(int index) => _items.RemoveAt(index);

    /// <inheritdoc />
    public override void RemoveAt(string parameterName)
    {
        var i = IndexOf(parameterName);
        if (i >= 0)
            _items.RemoveAt(i);
    }

    /// <inheritdoc />
    protected override DbParameter GetParameter(int index) => _items[index];

    /// <inheritdoc />
    protected override DbParameter GetParameter(string parameterName) => this[parameterName];

    /// <inheritdoc />
    protected override void SetParameter(int index, DbParameter value) =>
        this[index] = CastParameter(value);

    /// <inheritdoc />
    protected override void SetParameter(string parameterName, DbParameter value) =>
        this[parameterName] = CastParameter(value);

    /// <summary>
    /// Projects the collection into the <see cref="NamedScalarValueAndMetadata"/> sequence expected by
    /// <see cref="SessionContext.SqlAsync(string, System.Collections.Generic.IEnumerable{NamedScalarValueAndMetadata})"/>.
    /// </summary>
    internal IEnumerable<NamedScalarValueAndMetadata> ToDataFusionParameters() =>
        _items.Select(p => new NamedScalarValueAndMetadata(p.NormalizedName, p.ToScalarValue()));

    private int IndexOfChecked(string parameterName)
    {
        var i = IndexOf(parameterName);

#pragma warning disable CA2201
        if (i < 0)
            throw new IndexOutOfRangeException($"Parameter '{parameterName}' not found.");
#pragma warning restore CA2201

        return i;
    }

    private static DataFusionSharpParameter CastParameter(object value)
    {
        if (value is DataFusionSharpParameter p)
            return p;

        throw new ArgumentException($"Value must be a {nameof(DataFusionSharpParameter)}.", nameof(value));
    }
}

