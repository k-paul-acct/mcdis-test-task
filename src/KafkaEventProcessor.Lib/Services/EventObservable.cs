using System.Reactive.Subjects;
using KafkaEventProcessor.Lib.Models;

namespace KafkaEventProcessor.Lib.Services;

public sealed class EventObservable : IObservable<UserEvent>, IDisposable
{
    private readonly Subject<UserEvent> _subject;
    private bool _isDisposed;

    public EventObservable()
    {
        _subject = new Subject<UserEvent>();
    }

    public IDisposable Subscribe(IObserver<UserEvent> observer)
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
        return _subject.Subscribe(observer);
    }

    public void PublishEvent(UserEvent userEvent)
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
        _subject.OnNext(userEvent);
    }

    public void PublishError(Exception ex)
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
        _subject.OnError(ex);
    }

    public void Complete()
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
        _subject.OnCompleted();
    }

    public void Dispose()
    {
        if (_isDisposed)
        {
            return;
        }

        _subject.Dispose();
        _isDisposed = true;
    }
}
