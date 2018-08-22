import logging

from pytest import fixture

from firefighter.logging import FirehoseHandler


@fixture
def fx_make_handler(monkeypatch):
    def m(submit_batch, use_queues=False):
        monkeypatch.setattr(FirehoseHandler, '_submit_batch', submit_batch)
        return FirehoseHandler('foo', use_queues=use_queues)

    return m


def test_firehose_handler_emit(caplog, fx_make_handler):
    def mockreturn(self, messages, delivery_stream_name):
        count = getattr(mockreturn, 'called', None)
        if count is not None:
            setattr(mockreturn, 'called', count + 1)
        assert len(messages) == 1
        assert messages[0] == b'foobar'
        assert delivery_stream_name == 'foo'

    setattr(mockreturn, 'called', 0)
    record = logging.LogRecord('hello', 20, '', 0, 'foobar', tuple(), False)
    handler = fx_make_handler(mockreturn, use_queues=False)
    handler.emit(record)
    assert getattr(mockreturn, 'called', 0) == 1
    assert not caplog.records
