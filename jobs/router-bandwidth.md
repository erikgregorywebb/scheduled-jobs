# router-bandwidth.py setup

Collects household internet usage (bytes and packets up/down) from the home router over UPnP and uploads one row per minute to `s3://egw-data-dumps/router/bandwidth/` every hour. It needs no router password, but it does need to run **on the home network**, so it lives on an always-on machine at home instead of in GitHub Actions.

Only the standard library is needed to read the router; `boto3` is needed to upload.

## 1. Test it (any computer on the home Wi-Fi)

```bash
python3 jobs/router-bandwidth.py --once
```

You should see the router's running byte totals. Run it twice a minute apart and the numbers should go up.

If it says no gateway was found:
- Check that UPnP is turned on in the router's admin page (on Verizon routers it's usually under Advanced → Network Settings or Advanced → Utilities).
- Make sure you're on the main network, not the guest network, and that no VPN is running.
- If the router answers but discovery doesn't work, pass the description URL directly with `--location http://192.168.1.1:<port>/<path>.xml`. Running `--once` on another machine prints that URL.

To watch the collector without uploading anything (rows print when a minute ends, after `--upload-every` minutes):

```bash
python3 jobs/router-bandwidth.py --dry-run --upload-every 2
```

## 2. Run it continuously

It needs AWS credentials that boto3 can find: either `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in the environment or `~/.aws/credentials`. A key that can only `s3:PutObject` on `egw-data-dumps/router/*` is enough, and safer to leave on a home device than the main key.

**Raspberry Pi / Linux (systemd).** Save as `/etc/systemd/system/router-bandwidth.service`, adjusting the user and paths:

```ini
[Unit]
Description=Router bandwidth collector
After=network-online.target
Wants=network-online.target

[Service]
User=pi
EnvironmentFile=/home/pi/.router-bandwidth.env
ExecStart=/usr/bin/python3 /home/pi/scheduled-jobs/jobs/router-bandwidth.py
Restart=always
RestartSec=60

[Install]
WantedBy=multi-user.target
```

`/home/pi/.router-bandwidth.env` holds the two `AWS_...=` lines. Then:

```bash
sudo systemctl enable --now router-bandwidth
```

```bash
journalctl -u router-bandwidth -f
```

**Mac.** Works the same way under a `launchd` agent with `KeepAlive`, but the Mac must not sleep, or there will be gaps (shown as low `seconds_covered`).

## Output columns

| Column | Meaning |
| --- | --- |
| `minute` | Local minute the traffic falls in (`YYYY-MM-DD HH:MM`) |
| `bytes_received`, `bytes_sent` | Internet download / upload during that minute |
| `packets_received`, `packets_sent` | Same, in packets; blank if the router doesn't report them |
| `seconds_covered` | Seconds of the minute actually measured; under 60 means a gap (collector started, router unreachable, machine asleep) |
| `samples` | Readings taken during the minute |
| `counter_wraps` | Times a router counter rolled over and was corrected |
| `router_reboots` | Router restarts detected; traffic between the restart and the next reading isn't counted |
| `downstream_max_bps`, `upstream_max_bps` | Link speed the router reports |
| `router_uptime_s` | Router's connection uptime at the last reading |
| `raw_bytes_received`, `raw_bytes_sent` | Raw counter values at the last reading, for debugging |
| `datetime` | When the file was written |

## Limits

- Household totals only. UPnP doesn't break usage down by device.
- Counters on many routers are 32-bit and wrap every ~4.3 GB. Sampling every 10 seconds keeps up with sustained speeds up to ~3.4 Gbps; lower `--interval` if the line is faster.
