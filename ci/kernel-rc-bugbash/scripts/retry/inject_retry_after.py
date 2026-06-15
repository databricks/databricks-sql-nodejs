# mitmproxy addon: inject N x 429 (with Retry-After) on the SEA statement-submit
# POST, then pass through. Logs a timestamp for every faulted + passthrough hit
# so we can measure the client's inter-attempt backoff gaps.
#
# Run:  mitmdump -p 8080 -s inject_retry_after.py --set retry_after=5 --set faults=3
#       (mode = regular HTTP proxy; HTTPS is MITM'd with ~/.mitmproxy CA)
import time
from mitmproxy import http, ctx

STATE = {"hits": 0, "log": "/Users/madhavendra.rathore/kernel-bugbash/scripts/retry/attempts.log"}


def load(loader):
    loader.add_option("retry_after", int, 5, "Retry-After seconds to send on 429")
    loader.add_option("faults", int, 3, "How many leading statement-submit attempts to fault")
    loader.add_option("fault_status", int, 429, "HTTP status to inject (429 or 503)")
    loader.add_option("empty_body", bool, False, "Inject an empty body instead of a JSON error body")
    loader.add_option("force_retry_after", bool, False, "Always send Retry-After (even for 503)")
    loader.add_option("no_retry_after", bool, False, "Never send Retry-After (even for 429)")
    loader.add_option("target_method", str, "POST", "Which method to fault: POST (submit) or GET (poll)")
    # truncate the log at startup
    with open(STATE["log"], "w") as f:
        f.write("")


def _is_submit(flow: http.HTTPFlow) -> bool:
    # SEA statements endpoint. POST = submit (non-idempotent),
    # GET = poll status (idempotent). target_method selects which to fault.
    p = flow.request.path
    return flow.request.method == ctx.options.target_method and "/sql/statements" in p


def _logline(msg: str):
    line = f"{time.time():.3f}  {msg}\n"
    with open(STATE["log"], "a") as f:
        f.write(line)
    ctx.log.info("INJECT " + msg)


def request(flow: http.HTTPFlow):
    if not _is_submit(flow):
        return
    STATE["hits"] += 1
    n = STATE["hits"]
    if n <= ctx.options.faults:
        status = ctx.options.fault_status
        headers = {}
        send_ra = ((status == 429) or ctx.options.force_retry_after) and not ctx.options.no_retry_after
        if send_ra:
            headers["Retry-After"] = str(ctx.options.retry_after)
        if ctx.options.empty_body:
            body = b""
        else:
            headers["Content-Type"] = "application/json"
            body = b'{"error":"injected transient fault for retry test"}'
        _logline(f"submit attempt #{n} -> injected {status}"
                 + (f" Retry-After:{ctx.options.retry_after}" if send_ra else " (no Retry-After)")
                 + (" empty-body" if ctx.options.empty_body else " json-body"))
        flow.response = http.Response.make(status, body, headers)
    else:
        _logline(f"submit attempt #{n} -> PASSTHROUGH to upstream")
