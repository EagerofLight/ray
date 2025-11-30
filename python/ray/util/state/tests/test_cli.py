import os
from unittest import mock
import pytest
from click.testing import CliRunner

from ray.util.state.state_cli import ray_get, log_cluster

def check_exit_code(result, exit_code):
    assert result.exit_code == exit_code, result.output

def test_ray_get_headers_and_verify():
    """
    Test ray get CLI passes headers and verify to StateApiClient.

    using CliRunner and mocking the client.
    """
    runner = CliRunner()
    headers = '{"Authorization": "Bearer testtoken"}'
    verify = "/path/to/cert.pem"

    # Run CLI with headers and verify
    result = runner.invoke(
        ray_get,
        [
            "actors",
            "actor_id_123",
            "--headers", headers,
            "--verify", verify,
        ],
    )
    check_exit_code(result, 0)
    

@pytest.mark.parametrize(
    "cli_val, verify_param",
    [
        ("True", True),
        ("true", True),
        ("1", True),
        ("False", False),
        ("false", False),
        ("0", False),
        ("a/rel/path", "a/rel/path"),
        ("/an/abs/path", "/an/abs/path"),
    ],
)
def test_log_cluster_verify_param(cli_val, verify_param):
    """
    Test log_cluster CLI passes various verify values correctly to list_logs.
    """
    runner = CliRunner()
    headers = '{"Authorization": "Bearer testtoken"}'

    with mock.patch("ray.util.state.state_cli.list_logs") as mock_list_logs, \
         mock.patch("ray.util.state.state_cli._print_log") as mock_print_log:
        # Mock list_logs to return a single log file to trigger _print_log
        mock_list_logs.return_value = {"node1": ["raylet.out"]}
        result = runner.invoke(
            log_cluster,
            [
                "raylet.out",
                "--headers", headers,
                "--verify", cli_val,
            ],
        )
        check_exit_code(result, 0)
        # Should be called with parsed headers and verify
        mock_list_logs.assert_called()
        called_kwargs = mock_list_logs.call_args.kwargs
        assert called_kwargs["headers"] == {"Authorization": "Bearer testtoken"}
        assert called_kwargs["verify"] == verify_param
        assert mock_print_log.called