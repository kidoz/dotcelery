namespace DotCelery.Worker;

/// <summary>Controls the exception information persisted in a task result.</summary>
public enum ResultExceptionDetail
{
    /// <summary>Stores exception types.</summary>
    TypeOnly,

    /// <summary>Stores exception types and messages.</summary>
    Message,

    /// <summary>Stores exception types, messages, and stack traces.</summary>
    Full,
}
