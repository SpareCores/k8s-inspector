from k8s_inspector import launch_attempts
from k8s_inspector.model import LaunchAttempts

FAILED = """launch-attempts v1 tried=2 shown=2
us-east-1|us-east-1a|spot|aws.m7i.large|InsufficientCapacity|SpotMaxPriceTooLow|no spot capacity
us-east-1|us-east-1b|spot|aws.m7i.large|QuotaExceeded|VcpuLimitExceeded|you have requested more vCPU"""
LAUNCHED = """launch-attempts v1 tried=2 shown=2 launched=us-east-1|us-east-1c|spot|aws.m7i.large
us-east-1|us-east-1a|spot|aws.m7i.large|InsufficientCapacity|SpotMaxPriceTooLow|no spot capacity"""


def test_failed_create_has_one_outcome_per_line():
    parsed = launch_attempts.parse(FAILED)
    assert (parsed.tried, parsed.truncated) == (2, False)
    assert parsed.outcomes[0] == {
        "region": "us-east-1",
        "zone": "us-east-1a",
        "capacity_type": "spot",
        "instance_type": "aws.m7i.large",
        "outcome": "InsufficientCapacity",
        "code": "SpotMaxPriceTooLow",
        "message": "no spot capacity",
    }
    assert [o["outcome"] for o in parsed.outcomes] == ["InsufficientCapacity", "QuotaExceeded"]


def test_launched_candidate_comes_last_without_code_and_message():
    parsed = launch_attempts.parse(LAUNCHED)
    assert [o["outcome"] for o in parsed.outcomes] == ["InsufficientCapacity", "launched"]
    assert parsed.outcomes[-1] == {
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
    assert (parsed.tried, [o["outcome"] for o in parsed.outcomes]) == (1, ["launched"])


def test_truncated_flag_and_tried_beyond_shown():
    parsed = launch_attempts.parse(FAILED.replace("tried=2 shown=2", "tried=70 shown=2 truncated"))
    assert (parsed.tried, parsed.truncated, len(parsed.outcomes)) == (70, True, 2)


def test_unknown_version_or_garbage_is_ignored():
    assert launch_attempts.parse(FAILED.replace("v1", "v2")) is None
    assert launch_attempts.parse("something else entirely") is None
    assert launch_attempts.parse("launch-attempts v1 shown=1") is None
    assert launch_attempts.parse("") is None


def test_message_may_contain_slashes_where_pipes_were_and_empty_fields():
    parsed = launch_attempts.parse("launch-attempts v1 tried=1 shown=1\nr|z|spot|aws.m7i.large|Fatal||a/b/c: d")
    assert (parsed.outcomes[0]["code"], parsed.outcomes[0]["message"]) == ("", "a/b/c: d")


def test_a_message_with_more_pipes_keeps_them_in_the_message():
    parsed = launch_attempts.parse("launch-attempts v1 tried=1 shown=1\nr|z|spot|aws.m7i.large|Fatal|X|a|b|c")
    assert parsed.outcomes[0]["message"] == "a|b|c"


def test_a_line_with_too_few_fields_is_skipped():
    parsed = launch_attempts.parse("launch-attempts v1 tried=2 shown=2\nr|z|spot\n" + FAILED.splitlines()[1])
    assert [o["outcome"] for o in parsed.outcomes] == ["InsufficientCapacity"]


def test_merge_adds_up_the_claims_and_caps_the_outcomes():
    first = launch_attempts.merge(None, "bench-a", launch_attempts.parse(FAILED))
    assert (first.claims, first.tried, len(first.outcomes)) == (["bench-a"], 2, 2)
    many = LaunchAttempts(tried=60, outcomes=[{"outcome": "Fatal"}] * 49)
    second = launch_attempts.merge(first, "bench-b", launch_attempts.parse(LAUNCHED))
    assert (second.claims, second.tried, second.truncated) == (["bench-a", "bench-b"], 4, False)
    capped = launch_attempts.merge(many, "bench-c", launch_attempts.parse(FAILED))
    assert (capped.tried, len(capped.outcomes), capped.truncated) == (62, 50, True)
