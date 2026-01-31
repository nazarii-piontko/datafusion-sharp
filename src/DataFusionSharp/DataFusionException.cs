namespace DataFusionSharp;

public class DataFusionException : Exception
{
    public DataFusionException(string message) : base(message)
    {
    }

    public DataFusionException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
