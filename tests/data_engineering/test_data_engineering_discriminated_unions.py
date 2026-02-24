"""Tests for flow 076 -- Discriminated Unions."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "data_engineering_discriminated_unions",
    Path(__file__).resolve().parent.parent.parent
    / "flows"
    / "data_engineering"
    / "data_engineering_discriminated_unions.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["data_engineering_discriminated_unions"] = _mod
_spec.loader.exec_module(_mod)

EmailEvent = _mod.EmailEvent
WebhookEvent = _mod.WebhookEvent
ScheduleEvent = _mod.ScheduleEvent
ProcessingResult = _mod.ProcessingResult
parse_events = _mod.parse_events
process_email_event = _mod.process_email_event
process_webhook_event = _mod.process_webhook_event
process_schedule_event = _mod.process_schedule_event
route_event = _mod.route_event
summarize_processing = _mod.summarize_processing
discriminated_unions_flow = _mod.discriminated_unions_flow


def test_email_event_model() -> None:
    e = EmailEvent(event_id="e1", sender="a@b.com", recipient="c@d.com", subject="Hi")
    assert e.event_type == "email"


def test_webhook_event_model() -> None:
    e = WebhookEvent(event_id="w1", source_url="http://example.com", payload_size=100)
    assert e.event_type == "webhook"


def test_parse_events() -> None:
    raw = [
        {"event_type": "email", "event_id": "e1", "sender": "a", "recipient": "b", "subject": "s"},
        {"event_type": "webhook", "event_id": "w1", "source_url": "http://x", "payload_size": 10},
    ]
    events = parse_events.fn(raw)
    assert len(events) == 2
    assert isinstance(events[0], EmailEvent)
    assert isinstance(events[1], WebhookEvent)


def test_process_email() -> None:
    event = EmailEvent(event_id="e1", sender="a", recipient="b", subject="s")
    result = process_email_event.fn(event)
    assert result.status == "processed"
    assert result.event_type == "email"


def test_process_webhook() -> None:
    event = WebhookEvent(event_id="w1", source_url="http://x", payload_size=10)
    result = process_webhook_event.fn(event)
    assert result.status == "processed"


def test_route_email() -> None:
    event = EmailEvent(event_id="e1", sender="a", recipient="b", subject="s")
    result = route_event.fn(event)
    assert result.event_type == "email"


def test_route_schedule() -> None:
    event = ScheduleEvent(event_id="s1", cron_expression="* * * * *", job_name="job")
    result = route_event.fn(event)
    assert result.event_type == "schedule"


def test_flow_runs() -> None:
    state = discriminated_unions_flow(return_state=True)
    assert state.is_completed()
