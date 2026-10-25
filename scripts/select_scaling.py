#!/usr/bin/env python3
"""Measure checked SELECT scaling serially on an immutable existing seed."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

from select_cache_matrix import inventory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--dir', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--cache-bytes', type=int, default=20_000_000_000)
    parser.add_argument('--policy', choices=['lru', 'clock'], default='lru')
    parser.add_argument('--memory', choices=['heap', 'mmap'], default='mmap')
    parser.add_argument('--reuse-results', action='store_true')
    parser.add_argument('--workers', type=int, nargs='+', default=[1, 8, 16, 32, 60, 100])
    parser.add_argument('--seconds', type=int, default=15)
    parser.add_argument('--samples', type=int, default=3)
    args = parser.parse_args()
    api_mode = 'ExecPreparedInto' if args.reuse_results else 'ExecPrepared'
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {
        'started': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'host': platform.node(), 'cpus': os.cpu_count(),
        'api_mode': api_mode, 'reuse_results': args.reuse_results,
        'policy': args.policy, 'memory': args.memory,
        'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        'environment': {key: os.environ.get(key, 'runtime default') for key in ['GOGC', 'GOMEMLIMIT', 'GOMAXPROCS']},
        'seed': json.loads((args.dir / 'seed.json').read_text()),
        'inventory_before': inventory(args.dir), 'runs': [],
        'method': 'Serial fresh processes; same seed; page-spaced checked warmup; independently timed samples; uniform prepared point SELECTs; every answer checked. CPU includes process user/system time during timed samples. OS filesystem cache enabled, never reset. Decimal cache bytes.',
    }

    def save():
        (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')

    save()
    for workers in args.workers:
        label = f'workers-{workers}'
        destination = args.output / (label + '.json')
        command = [str(args.binary.resolve()), '-dir', str(args.dir.resolve()), '-cache-bytes', str(args.cache_bytes), '-cache-policy', args.policy, '-cache-memory', args.memory, '-workers', str(workers), '-duration', f'{args.seconds}s', '-samples', str(args.samples), '-json', str(destination.resolve())]
        if args.reuse_results:
            command.append('-reuse-results')
        print(' '.join(command), flush=True)
        with (args.output / (label + '.log')).open('w') as log:
            subprocess.run(['/usr/bin/time', '-v', '-o', str(args.output / (label + '.resources.txt')), *command], stdout=log, stderr=subprocess.STDOUT, check=True)
        report = json.loads(destination.read_text())
        assert not report.get('Error') and len(report['Samples']) == args.samples
        assert report['Config'].get('ReuseResults', False) == args.reuse_results
        assert report.get('APIMode', api_mode) == api_mode
        for sample in report['Samples']:
            assert sample['Queries'] > 0 and sample['AverageCPUCores'] > 0
            assert sample['Before']['SeqNum'] == sample['After']['SeqNum']
            assert sample['After']['BlockCacheBytes'] <= args.cache_bytes
            assert sample['After']['BlockCachePolicy'] == args.policy
            assert sample['After']['BlockCacheMemory'] == args.memory
        row = {'workers': workers, 'api_mode': api_mode, 'command': command, 'report': destination.name,
               'median_selects_per_second': statistics.median(s['QueriesPerSecond'] for s in report['Samples']),
               'median_cpu_cores': statistics.median(s['AverageCPUCores'] for s in report['Samples']),
               'median_cpu_us_per_query': statistics.median(s['CPUSeconds'] * 1e6 / s['Queries'] for s in report['Samples']),
               'median_gc_pause_seconds': statistics.median(s['GCPauseSeconds'] for s in report['Samples']),
               'median_voluntary_context_switches_per_query': statistics.median(s['VoluntaryContextSwitches'] / s['Queries'] for s in report['Samples']),
               'median_involuntary_context_switches_per_query': statistics.median(s['InvoluntaryContextSwitches'] / s['Queries'] for s in report['Samples']),
               'median_p99_us_upper_bound': statistics.median(s['P99US'] for s in report['Samples'])}
        manifest['runs'].append(row)
        save()
        print(f"{workers} readers: {row['median_selects_per_second']:.0f} SELECT/s; {row['median_cpu_cores']:.1f} CPU cores", flush=True)
    manifest['inventory_after'] = inventory(args.dir)
    manifest['sstables_unchanged'] = manifest['inventory_after'] == manifest['inventory_before']
    save()
    assert manifest['sstables_unchanged'], 'seed SSTables changed'


if __name__ == '__main__':
    main()
