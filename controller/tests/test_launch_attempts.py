from datetime import UTC, datetime

from k8s_inspector import launch_attempts
from k8s_inspector.model import LaunchEvent

FAILED = """launch-attempts v1 tried=2 shown=2
us-east-1|us-east-1a|spot|aws.m7i.large|InsufficientCapacity|SpotMaxPriceTooLow|no spot capacity
us-east-1|us-east-1b|spot|aws.m7i.large|QuotaExceeded|VcpuLimitExceeded|you have requested more vCPU"""
LAUNCHED = """launch-attempts v1 tried=2 shown=2 launched=us-east-1|us-east-1c|spot|aws.m7i.large
us-east-1|us-east-1a|spot|aws.m7i.large|InsufficientCapacity|SpotMaxPriceTooLow|no spot capacity"""


def test_failed_create_has_one_outcome_per_line():
    parsed = launch_attempts.parse(FAILED)
    assert (parsed.tried, parsed.truncated) == (2, False)
    assert parsed.failed[0] == {
        "region": "us-east-1",
        "zone": "us-east-1a",
        "capacity_type": "spot",
        "instance_type": "aws.m7i.large",
        "outcome": "InsufficientCapacity",
        "code": "SpotMaxPriceTooLow",
        "message": "no spot capacity",
    }
    assert [o["outcome"] for o in parsed.failed] == ["InsufficientCapacity", "QuotaExceeded"]


def test_launched_candidate_has_no_code_and_message():
    parsed = launch_attempts.parse(LAUNCHED)
    assert [o["outcome"] for o in parsed.failed] == ["InsufficientCapacity"]
    assert parsed.launched == {
        "region": "us-east-1",
        "zone": "us-east-1c",
        "capacity_type": "spot",
        "instance_type": "aws.m7i.large",
        "outcome": "launched",
        "code": None,
        "message": None,
    }


def test_launched_alone_has_no_lines():
    parsed = launch_attempts.parse("launch-attempts v1 tried=1 shown=1 launched=r|z|on-demand|aws.t3.micro")
    assert (parsed.tried, parsed.failed, parsed.launched["outcome"]) == (1, [], "launched")


def test_truncated_flag_and_tried_beyond_shown():
    parsed = launch_attempts.parse(FAILED.replace("tried=2 shown=2", "tried=70 shown=2 truncated"))
    assert (parsed.tried, parsed.truncated, len(parsed.failed)) == (70, True, 2)


def test_unknown_version_or_garbage_is_ignored():
    assert launch_attempts.parse(FAILED.replace("v1", "v2")) is None
    assert launch_attempts.parse("something else entirely") is None
    assert launch_attempts.parse("launch-attempts v1 shown=1") is None
    assert launch_attempts.parse("") is None


def test_message_may_contain_slashes_where_pipes_were_and_empty_fields():
    parsed = launch_attempts.parse("launch-attempts v1 tried=1 shown=1\nr|z|spot|aws.m7i.large|Fatal||a/b/c: d")
    assert (parsed.failed[0]["code"], parsed.failed[0]["message"]) == ("", "a/b/c: d")


def test_a_message_with_more_pipes_keeps_them_in_the_message():
    parsed = launch_attempts.parse("launch-attempts v1 tried=1 shown=1\nr|z|spot|aws.m7i.large|Fatal|X|a|b|c")
    assert parsed.failed[0]["message"] == "a|b|c"


def test_a_line_with_too_few_fields_is_skipped():
    parsed = launch_attempts.parse("launch-attempts v1 tried=2 shown=2\nr|z|spot\n" + FAILED.splitlines()[1])
    assert [o["outcome"] for o in parsed.failed] == ["InsufficientCapacity"]


def test_combined_prefix_of_an_aggregated_event_is_stripped():
    parsed = launch_attempts.parse("(combined from similar events): " + FAILED)
    assert (parsed.tried, len(parsed.failed)) == (2, 2)


def event(name, minute, message):
    parsed = launch_attempts.parse(message)
    return LaunchEvent(
        name,
        datetime(2026, 11, 27, 10, minute, tzinfo=UTC),
        parsed.tried,
        parsed.truncated,
        parsed.failed,
        parsed.launched,
    )


def test_summary_orders_events_by_time_and_puts_the_launched_one_last():
    later = event("c.2", 5, LAUNCHED)
    earlier = event("c.1", 1, FAILED)
    out = launch_attempts.summary([later, earlier])
    assert [o["outcome"] for o in out["launch_attempts"]] == [
        "InsufficientCapacity",
        "QuotaExceeded",
        "InsufficientCapacity",
        "launched",
    ]
    assert (out["launch_attempts_tried"], out["launch_attempts_truncated"]) == (4, False)


def test_add_cuts_the_failed_candidates_at_50_but_keeps_the_launched_one():
    many = "launch-attempts v1 tried=50 shown=50\n" + "\n".join(["r|z|spot|aws.m7i.large|Fatal|X|m"] * 50)
    events = launch_attempts.add([], event("c.1", 1, many))
    events = launch_attempts.add(events, event("c.2", 2, LAUNCHED))
    out = launch_attempts.summary(events)
    assert len(out["launch_attempts"]) == 51  # 50 failed, then the launched one
    assert out["launch_attempts"][-1]["outcome"] == "launched"
    assert out["launch_attempts_truncated"] is True and events[1].failed == []
