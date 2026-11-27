"""Collect household internet usage from the home router over UPnP.

Unlike the other jobs, this one can't run on GitHub Actions: the router is
only reachable from inside the home network. Run it on an always-on machine
at home (Raspberry Pi, old laptop, Mac mini). See jobs/router-bandwidth.md.

The router's UPnP Internet Gateway Device service reports running totals of
bytes and packets sent/received on the WAN (internet) side, with no login.
Those counters are often 32-bit and wrap every ~4.3 GB (about 34 seconds at
1 Gbps), and they reset when the router reboots, so this script samples them
every few seconds, turns consecutive readings into deltas (correcting for
wraps and reboots), sums the deltas into one row per minute, and uploads the
rows to S3 once an hour.

Household totals only: UPnP doesn't break usage down by device.

Usage:
    python router-bandwidth.py --once      # discover the router, print one reading
    python router-bandwidth.py --dry-run   # run the collector, print rows instead of uploading
    python router-bandwidth.py             # run the collector, upload to S3 hourly
"""

import argparse
import csv
import os
import re
import signal
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from io import StringIO

BUCKET = 'egw-data-dumps'
S3_PREFIX = 'router/bandwidth/router-bandwidth-'

SSDP_ADDR = ('239.255.255.250', 1900)
SSDP_TARGETS = [
    'urn:schemas-upnp-org:device:InternetGatewayDevice:2',
    'urn:schemas-upnp-org:device:InternetGatewayDevice:1',
    'urn:schemas-upnp-org:service:WANCommonInterfaceConfig:1',
]
WAN_COMMON = 'urn:schemas-upnp-org:service:WANCommonInterfaceConfig:1'
# connection services that report router uptime, used to detect reboots
WAN_CONNECTIONS = [
    'urn:schemas-upnp-org:service:WANIPConnection:2',
    'urn:schemas-upnp-org:service:WANIPConnection:1',
    'urn:schemas-upnp-org:service:WANPPPConnection:1',
]

COUNTERS = ['bytes_received', 'bytes_sent', 'packets_received', 'packets_sent']
COUNTER_ACTIONS = {
    'bytes_received': ('GetTotalBytesReceived', 'NewTotalBytesReceived'),
    'bytes_sent': ('GetTotalBytesSent', 'NewTotalBytesSent'),
    'packets_received': ('GetTotalPacketsReceived', 'NewTotalPacketsReceived'),
    'packets_sent': ('GetTotalPacketsSent', 'NewTotalPacketsSent'),
}

FIELDS = [
    'minute', 'bytes_received', 'bytes_sent', 'packets_received', 'packets_sent',
    'seconds_covered', 'samples', 'counter_wraps', 'router_reboots',
    'downstream_max_bps', 'upstream_max_bps', 'router_uptime_s',
    'raw_bytes_received', 'raw_bytes_sent', 'datetime',
]


# ---------------------------------------------------------------- discovery

def discover(timeout=3.0):
    """Return the description-XML URL of the first gateway answering SSDP."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    sock.settimeout(timeout)
    try:
        for target in SSDP_TARGETS:
            msg = (
                'M-SEARCH * HTTP/1.1\r\n'
                f'HOST: {SSDP_ADDR[0]}:{SSDP_ADDR[1]}\r\n'
                'MAN: "ssdp:discover"\r\n'
                'MX: 2\r\n'
                f'ST: {target}\r\n\r\n'
            )
            sock.sendto(msg.encode(), SSDP_ADDR)
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                data, _ = sock.recvfrom(65507)
            except socket.timeout:
                break
            match = re.search(rb'(?im)^location:\s*(\S+)', data)
            if match:
                return match.group(1).decode()
    finally:
        sock.close()
    return None


def strip_ns(tag):
    return tag.split('}', 1)[-1]


def find_services(location):
    """Map serviceType -> absolute control URL from the device description."""
    with urllib.request.urlopen(location, timeout=10) as resp:
        root = ET.fromstring(resp.read())
    base = location
    for el in root.iter():
        if strip_ns(el.tag) == 'URLBase' and el.text and el.text.strip():
            base = el.text.strip()
    services = {}
    for el in root.iter():
        if strip_ns(el.tag) != 'service':
            continue
        fields = {strip_ns(child.tag): (child.text or '').strip() for child in el}
        if fields.get('serviceType') and fields.get('controlURL'):
            services[fields['serviceType']] = urllib.parse.urljoin(base, fields['controlURL'])
    return services


# --------------------------------------------------------------------- SOAP

class SoapFault(Exception):
    pass


def soap_call(control_url, service_type, action, timeout=5):
    body = (
        '<?xml version="1.0"?>'
        '<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" '
        's:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">'
        f'<s:Body><u:{action} xmlns:u="{service_type}"/></s:Body></s:Envelope>'
    ).encode()
    req = urllib.request.Request(control_url, data=body, method='POST', headers={
        'Content-Type': 'text/xml; charset="utf-8"',
        'SOAPAction': f'"{service_type}#{action}"',
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            root = ET.fromstring(resp.read())
    except urllib.error.HTTPError as e:  # SOAP faults come back as HTTP 500
        raise SoapFault(f'{action}: HTTP {e.code}') from e
    return {strip_ns(el.tag): (el.text or '').strip() for el in root.iter()}


class Gateway:
    def __init__(self, location):
        self.location = location
        services = find_services(location)
        if WAN_COMMON not in services:
            raise RuntimeError(
                f'{location} has no {WAN_COMMON} service; found: {sorted(services)}')
        self.common_url = services[WAN_COMMON]
        self.conn = next(((s, services[s]) for s in WAN_CONNECTIONS if s in services), None)
        # find out once which optional actions the router supports
        self.supported = []
        for name, (action, key) in COUNTER_ACTIONS.items():
            try:
                int(soap_call(self.common_url, WAN_COMMON, action)[key])
                self.supported.append(name)
            except (SoapFault, KeyError, ValueError):
                pass
        if 'bytes_received' not in self.supported or 'bytes_sent' not in self.supported:
            raise RuntimeError('router does not report total bytes sent/received over UPnP')
        self.has_uptime = self.uptime() is not None

    def uptime(self):
        if not self.conn:
            return None
        try:
            return int(soap_call(self.conn[1], self.conn[0], 'GetStatusInfo')['NewUptime'])
        except (SoapFault, KeyError, ValueError, OSError):
            return None

    def link(self):
        try:
            r = soap_call(self.common_url, WAN_COMMON, 'GetCommonLinkProperties')
            return int(r['NewLayer1DownstreamMaxBitRate']), int(r['NewLayer1UpstreamMaxBitRate'])
        except (SoapFault, KeyError, ValueError, OSError):
            return None, None

    def read(self):
        reading = {'time': time.time()}
        for name in self.supported:
            action, key = COUNTER_ACTIONS[name]
            reading[name] = int(soap_call(self.common_url, WAN_COMMON, action)[key])
        reading['uptime'] = self.uptime() if self.has_uptime else None
        return reading


# ------------------------------------------------------------ delta logic

def counter_delta(prev, cur, modulus, rebooted):
    """Return (delta, wrapped) between two readings of a running counter."""
    if rebooted:
        return 0, False  # unknown how much was counted between reboot and now
    if cur >= prev:
        return cur - prev, False
    # counter went down: a wrap if it was near the top, otherwise a reset
    if prev > modulus // 2:
        return cur + modulus - prev, True
    return 0, False


class Collector:
    def __init__(self, gateway):
        self.gateway = gateway
        self.prev = None
        self.modulus = 2 ** 32
        self.minutes = {}  # minute label -> accumulating row
        self.downstream, self.upstream = gateway.link()

    def sample(self):
        cur = self.gateway.read()
        if any(cur.get(c, 0) >= 2 ** 32 for c in COUNTERS):
            self.modulus = 2 ** 64  # router keeps 64-bit counters
        prev, self.prev = self.prev, cur
        if prev is None:
            return
        rebooted = (
            cur['uptime'] is not None and prev['uptime'] is not None
            and cur['uptime'] < prev['uptime']
        )
        minute = datetime.fromtimestamp(cur['time']).strftime('%Y-%m-%d %H:%M')
        row = self.minutes.setdefault(minute, {
            'minute': minute, **{c: 0 for c in COUNTERS},
            'seconds_covered': 0.0, 'samples': 0, 'counter_wraps': 0, 'router_reboots': 0,
        })
        reset = False
        for c in self.gateway.supported:
            d, wrapped = counter_delta(prev[c], cur[c], self.modulus, rebooted)
            row[c] += d
            row['counter_wraps'] += wrapped
            if not wrapped and cur[c] < prev[c]:
                reset = True
        row['router_reboots'] += rebooted or reset
        row['seconds_covered'] += cur['time'] - prev['time']
        row['samples'] += 1
        row['router_uptime_s'] = cur['uptime']
        row['raw_bytes_received'] = cur['bytes_received']
        row['raw_bytes_sent'] = cur['bytes_sent']
        if rebooted or reset:  # link speed may have changed
            self.downstream, self.upstream = self.gateway.link()

    def pop_finished(self, include_current=False):
        """Remove and return rows for minutes that have ended."""
        now_minute = datetime.now().strftime('%Y-%m-%d %H:%M')
        done = sorted(m for m in self.minutes if include_current or m < now_minute)
        rows = []
        for m in done:
            row = self.minutes.pop(m)
            row['seconds_covered'] = round(row['seconds_covered'], 1)
            row['downstream_max_bps'] = self.downstream
            row['upstream_max_bps'] = self.upstream
            for c in COUNTERS:
                if c not in self.gateway.supported:
                    row[c] = None
            rows.append(row)
        return rows


# ------------------------------------------------------------------- output

def to_csv(rows):
    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=FIELDS, extrasaction='ignore')
    writer.writeheader()
    stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for row in rows:
        writer.writerow({**row, 'datetime': stamp})
    return buf.getvalue()


def upload(rows, dry_run):
    body = to_csv(rows)
    if dry_run:
        print(body, end='', flush=True)
        return
    import boto3  # imported here so --once and --dry-run work without it
    key = S3_PREFIX + datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + '.csv'
    boto3.client('s3').put_object(Bucket=BUCKET, Key=key, Body=body)
    print(f'Copy {len(rows)} rows to S3 Bucket {BUCKET} at {key}, Done!', flush=True)


def connect(location):
    while True:
        try:
            loc = location or discover()
            if not loc:
                raise RuntimeError('no UPnP gateway answered discovery')
            gw = Gateway(loc)
            print(f'Using gateway at {loc} (counters: {", ".join(gw.supported)}; '
                  f'uptime: {"yes" if gw.has_uptime else "no"})', flush=True)
            return gw
        except (OSError, RuntimeError, ET.ParseError) as e:
            print(f'Could not connect to router ({e}); retrying in 60s', flush=True)
            time.sleep(60)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--once', action='store_true', help='print one reading and exit')
    parser.add_argument('--dry-run', action='store_true', help='print rows instead of uploading')
    parser.add_argument('--interval', type=float, default=10, help='seconds between samples')
    parser.add_argument('--upload-every', type=int, default=60, help='minutes between uploads')
    parser.add_argument('--location', help='device description URL; skips SSDP discovery')
    args = parser.parse_args()

    if args.once:
        loc = args.location or discover()
        if not loc:
            sys.exit('No UPnP gateway found. Is UPnP turned on in the router settings, '
                     'and is this machine on the home network (not a guest network or VPN)?')
        gw = Gateway(loc)
        down, up = gw.link()
        print(f'Gateway description: {loc}')
        print(f'Counters supported:  {", ".join(gw.supported)}')
        for name, value in gw.read().items():
            if name != 'time':
                print(f'{name + ":":20} {value}')
        print(f'{"link down/up bps:":20} {down} / {up}')
        return

    stop = []
    signal.signal(signal.SIGTERM, lambda *_: stop.append(True))

    gateway = connect(args.location)
    collector = Collector(gateway)
    pending = []
    last_upload = time.time()
    failures = 0
    try:
        while not stop:
            started = time.time()
            try:
                collector.sample()
                failures = 0
            except (OSError, SoapFault, ET.ParseError, KeyError, ValueError) as e:
                failures += 1
                print(f'Sample failed ({e})', flush=True)
                if failures >= 6:  # router may have changed address after a reboot
                    collector.gateway = connect(args.location)
                    collector.prev = None
                    failures = 0
            pending += collector.pop_finished()
            if pending and time.time() - last_upload >= args.upload_every * 60:
                try:
                    upload(pending, args.dry_run)
                    pending = []
                except Exception as e:  # keep the rows and try again next time
                    print(f'Upload failed ({e}); keeping {len(pending)} rows', flush=True)
                last_upload = time.time()
            time.sleep(max(0, args.interval - (time.time() - started)))
    except KeyboardInterrupt:
        pass
    pending += collector.pop_finished(include_current=True)
    if pending:
        upload(pending, args.dry_run)


if __name__ == '__main__':
    main()
