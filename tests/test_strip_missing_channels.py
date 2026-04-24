"""Tests for _strip_missing_channel_refs in processing.py.

We import the function via importlib to avoid pulling in the full
processing module (which requires emeraldprocessing and other heavy deps).
"""

import importlib.util
import os
import unittest


def _load_function():
    """Load _strip_missing_channel_refs directly from source without full module import."""
    src = os.path.join(
        os.path.dirname(__file__), os.pardir,
        "beryl_pipeline", "processing.py",
    )
    src = os.path.abspath(src)
    # Read source and extract the function
    with open(src) as f:
        source = f.read()

    # Extract just the function definition
    lines = source.split('\n')
    func_lines = []
    in_func = False
    for line in lines:
        if line.startswith('def _strip_missing_channel_refs('):
            in_func = True
        if in_func:
            func_lines.append(line)
            # End of function: next non-empty, non-indented line
            if func_lines and len(func_lines) > 1 and line and not line[0].isspace() and not line.startswith('def _strip'):
                func_lines.pop()  # remove the non-function line
                break

    func_source = '\n'.join(func_lines)
    ns = {}
    exec(func_source, ns)
    return ns['_strip_missing_channel_refs']


_strip_missing_channel_refs = _load_function()


class TestStripMissingChannelRefs(unittest.TestCase):
    """Test that _strip_missing_channel_refs correctly removes missing channel references.

    Steps use the pipeline format: [{"module.function_name": {args...}}, ...]
    """

    def test_strips_ch02_when_only_ch01_present(self):
        """Single-moment data: Gate_Ch02 refs should be removed."""
        steps = [
            {
                "emeraldprocessing.pipeline.cull_on_geometry": {
                    "distance": {
                        "Gate_Ch01": 10.0,
                        "Gate_Ch02": 20.0,
                    }
                },
            }
        ]
        available = {"Gate_Ch01"}
        _strip_missing_channel_refs(steps, available)
        self.assertEqual(
            steps[0]["emeraldprocessing.pipeline.cull_on_geometry"]["distance"],
            {"Gate_Ch01": 10.0},
        )

    def test_keeps_both_channels_when_dual_moment(self):
        """Dual-moment data: both channels should be kept."""
        steps = [
            {
                "emeraldprocessing.pipeline.cull_on_geometry": {
                    "distance": {
                        "Gate_Ch01": 10.0,
                        "Gate_Ch02": 20.0,
                    }
                },
            }
        ]
        available = {"Gate_Ch01", "Gate_Ch02"}
        _strip_missing_channel_refs(steps, available)
        self.assertEqual(
            steps[0]["emeraldprocessing.pipeline.cull_on_geometry"]["distance"],
            {"Gate_Ch01": 10.0, "Gate_Ch02": 20.0},
        )

    def test_non_gate_keys_preserved(self):
        """Non-Gate_Ch keys should never be stripped."""
        steps = [
            {
                "emeraldprocessing.pipeline.some_step": {
                    "params": {
                        "Gate_Ch01": 1.0,
                        "Gate_Ch02": 2.0,
                        "other_key": 3.0,
                    }
                },
            }
        ]
        available = {"Gate_Ch01"}
        _strip_missing_channel_refs(steps, available)
        args = steps[0]["emeraldprocessing.pipeline.some_step"]
        self.assertIn("other_key", args["params"])
        self.assertNotIn("Gate_Ch02", args["params"])

    def test_no_args_step_is_skipped(self):
        """Steps with non-dict args should be silently skipped."""
        steps = [{"emeraldprocessing.pipeline.simple_step": "no_args"}]
        available = {"Gate_Ch01"}
        _strip_missing_channel_refs(steps, available)  # should not raise

    def test_string_step_is_skipped(self):
        """Non-dict steps (e.g., string step names) should be skipped."""
        steps = ["some_step_name"]
        available = {"Gate_Ch01"}
        _strip_missing_channel_refs(steps, available)  # should not raise

    def test_log_fn_called_on_strip(self):
        """log_fn should be called for each stripped key."""
        logged = []
        steps = [
            {
                "emeraldprocessing.pipeline.cull_on_geometry": {
                    "distance": {
                        "Gate_Ch01": 10.0,
                        "Gate_Ch02": 20.0,
                    }
                },
            }
        ]
        available = {"Gate_Ch01"}
        _strip_missing_channel_refs(steps, available, log_fn=logged.append)
        self.assertEqual(len(logged), 1)
        self.assertIn("Gate_Ch02", logged[0])
        self.assertIn("cull_on_geometry", logged[0])

    def test_multiple_steps_processed(self):
        """All steps in the list should be processed."""
        steps = [
            {
                "emeraldprocessing.pipeline.step1": {
                    "d": {"Gate_Ch01": 1, "Gate_Ch02": 2}
                },
            },
            {
                "emeraldprocessing.pipeline.step2": {
                    "d": {"Gate_Ch01": 3, "Gate_Ch02": 4}
                },
            },
        ]
        available = {"Gate_Ch01"}
        _strip_missing_channel_refs(steps, available)
        self.assertEqual(
            steps[0]["emeraldprocessing.pipeline.step1"]["d"], {"Gate_Ch01": 1}
        )
        self.assertEqual(
            steps[1]["emeraldprocessing.pipeline.step2"]["d"], {"Gate_Ch01": 3}
        )

    def test_empty_steps_list(self):
        """Empty steps list should not raise."""
        _strip_missing_channel_refs([], {"Gate_Ch01"})


if __name__ == "__main__":
    unittest.main()
