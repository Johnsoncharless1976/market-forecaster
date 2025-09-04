#!/usr/bin/env python3
"""
Stability Mode: Twin Daily Batches (AM + EOD)
Artifact-only dashboard with batched releases
"""

import os
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
import json


class StabilityBatchSystem:
    """Twin daily batch system with artifact-only dashboard"""
    
    def __init__(self):
        # Stability mode configuration
        self.stability_mode = os.getenv('STABILITY_MODE', 'false').lower() == 'true'
        self.batch_am_enabled = os.getenv('BATCH_AM_ENABLED', 'true').lower() == 'true'
        self.batch_pm_enabled = os.getenv('BATCH_PM_ENABLED', 'true').lower() == 'true'
        
        # Timing configuration (ET)
        self.am_preview_time = os.getenv('AM_PREVIEW_TIME', '08:40')
        self.am_send_time = os.getenv('AM_SEND_TIME', '09:00')
        self.am_send_time_macro = os.getenv('AM_SEND_TIME_MACRO', '09:15')
        self.pm_preview_time = os.getenv('PM_PREVIEW_TIME', '16:45')
        self.pm_send_time = os.getenv('PM_SEND_TIME', '17:00')
        self.eod_drop_time = os.getenv('EOD_DROP_TIME', '17:10')
        
        # Required artifacts for each batch
        self.batch_artifacts = [
            'CONFIDENCE_STRIP.md',
            'CONFIDENCE_SPARKLINE.md', 
            'SHADOW_SCORECARD.md',
            'TODAY_GLANCE.md',
            'ZEN_COUNCIL_EXPLAIN.md',
            'NEWS_SCORE.md',
            'MACRO_EVENTS.md',
            'LEVEL_MAGNETS.md',
            'LEARNING_LOG.md',
            'PARAM_HISTORY.md',
            'WIN_GATE.md',
            'REPO_HEALTH.md'
        ]
        
        # Directory structure
        self.audit_base = Path('audit_exports/daily')
        
    def check_stability_mode(self):
        """Check stability mode configuration"""
        return {
            'stability_mode': self.stability_mode,
            'batch_am_enabled': self.batch_am_enabled,
            'batch_pm_enabled': self.batch_pm_enabled,
            'am_times': {
                'preview': self.am_preview_time,
                'send': self.am_send_time,
                'send_macro': self.am_send_time_macro
            },
            'pm_times': {
                'preview': self.pm_preview_time,
                'send': self.pm_send_time,
                'eod_drop': self.eod_drop_time
            }
        }
    
    def find_latest_artifacts(self):
        """Find latest artifact timestamp directory"""
        try:
            timestamp_dirs = [d for d in self.audit_base.iterdir() 
                            if d.is_dir() and d.name.match(r'\d{8}_\d{6}')]
            if not timestamp_dirs:
                return None
            return sorted(timestamp_dirs, key=lambda x: x.name)[-1]
        except:
            return None
    
    def collect_batch_artifacts(self, artifact_dir):
        """Collect available artifacts for batch"""
        available_artifacts = {}
        missing_artifacts = []
        
        if not artifact_dir or not artifact_dir.exists():
            return available_artifacts, self.batch_artifacts.copy()
        
        for artifact_name in self.batch_artifacts:
            artifact_file = artifact_dir / artifact_name
            if artifact_file.exists():
                available_artifacts[artifact_name] = artifact_file
            else:
                missing_artifacts.append(artifact_name)
        
        return available_artifacts, missing_artifacts
    
    def create_batch_drop(self, batch_type, available_artifacts, missing_artifacts):
        """Create AM or EOD batch drop"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create batch directory structure
        batch_dir = self.audit_base / timestamp / 'batches' / batch_type
        batch_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate batch metadata
        batch_meta = {
            'batch_type': batch_type,
            'generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
            'sha': os.getenv('CI_COMMIT_SHORT_SHA', 'local'),
            'artifacts_included': len(available_artifacts),
            'artifacts_missing': len(missing_artifacts),
            'stability_mode': self.stability_mode
        }
        
        # Write batch drop markdown
        drop_file = batch_dir / f'{batch_type}_DROP.md'
        
        content = f"""# {batch_type} Batch Drop

**Generated**: {batch_meta['generated']}
**Batch Type**: {batch_type} ({'AM Market Open' if batch_type == 'AM' else 'End of Day'})
**Pipeline SHA**: {batch_meta['sha']}
**Stability Mode**: {'ON' if self.stability_mode else 'OFF'}

## Batch Contents

### Included Artifacts ({len(available_artifacts)}/{len(self.batch_artifacts)})
"""
        
        # List included artifacts
        for artifact_name, artifact_file in available_artifacts.items():
            file_size = artifact_file.stat().st_size if artifact_file.exists() else 0
            content += f"- ✅ **{artifact_name}** ({file_size:,} bytes)\n"
        
        # List missing artifacts
        if missing_artifacts:
            content += f"\n### Missing Artifacts ({len(missing_artifacts)})\n"
            for artifact_name in missing_artifacts:
                content += f"- ❌ **{artifact_name}** (not available)\n"
        
        content += f"""

## Batch Schedule

### {batch_type} Timing
"""
        
        if batch_type == 'AM':
            content += f"""- **Preview**: {self.am_preview_time} ET
- **Email Send**: {self.am_send_time} ET (or {self.am_send_time_macro} ET if Macro Gate active)
- **Dashboard Update**: After email send
- **Artifact Lock**: Until EOD batch
"""
        else:
            content += f"""- **Preview**: {self.pm_preview_time} ET  
- **Email Send**: {self.pm_send_time} ET
- **EOD Drop**: {self.eod_drop_time} ET
- **Dashboard Update**: After EOD drop
- **Artifact Lock**: Until next AM batch
"""
        
        content += f"""

## Dashboard Integration

- **Source Display**: SHA {batch_meta['sha']} @ {batch_meta['generated']}
- **Batch Label**: {batch_type} batch artifacts
- **Missing Handling**: "Awaiting batch" badges for unavailable tiles
- **Update Frequency**: Batch-driven only (no real-time updates)

## Stability Mode

- **Mode**: {'ACTIVE' if self.stability_mode else 'INACTIVE'}
- **CI Guard**: {'ENABLED' if self.stability_mode else 'DISABLED'} (blocks non-HOTFIX changes)
- **Batch Lock**: Artifacts frozen until next scheduled batch
- **Hotfix Override**: Available with ALLOW_HOTFIX=true

## Artifact Summary

| Component | Status | Description |
|-----------|---------|-------------|
| **Confidence** | {'✅ Available' if 'CONFIDENCE_STRIP.md' in available_artifacts else '⏳ Awaiting'} | Strip + sparkline data |
| **Shadow Scorecard** | {'✅ Available' if 'SHADOW_SCORECARD.md' in available_artifacts else '⏳ Awaiting'} | 30-day cohort performance |
| **Today Glance** | {'✅ Available' if 'TODAY_GLANCE.md' in available_artifacts else '⏳ Awaiting'} | Overview row data |
| **Council** | {'✅ Available' if 'ZEN_COUNCIL_EXPLAIN.md' in available_artifacts else '⏳ Awaiting'} | Decision explanations |
| **News/Macro** | {'✅ Available' if 'NEWS_SCORE.md' in available_artifacts and 'MACRO_EVENTS.md' in available_artifacts else '⏳ Awaiting'} | Market conditions |
| **Systems** | {'✅ Available' if 'LEVEL_MAGNETS.md' in available_artifacts and 'WIN_GATE.md' in available_artifacts else '⏳ Awaiting'} | Trading system status |

---
**{batch_type} BATCH DROP**: {'Complete' if not missing_artifacts else f'{len(available_artifacts)}/{len(self.batch_artifacts)} artifacts'}
Generated by Stability Batch System v1.0
"""
        
        # Write the drop file
        with open(drop_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        # Copy available artifacts to batch directory
        for artifact_name, artifact_file in available_artifacts.items():
            if artifact_file.exists():
                shutil.copy2(artifact_file, batch_dir / artifact_name)
        
        # Create batch zip
        zip_file = batch_dir / f'{batch_type}_DROP.zip'
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Add drop markdown
            zf.write(drop_file, f'{batch_type}_DROP.md')
            
            # Add artifacts
            for artifact_name, artifact_file in available_artifacts.items():
                if artifact_file.exists():
                    zf.write(artifact_file, artifact_name)
        
        return {
            'drop_file': str(drop_file),
            'zip_file': str(zip_file),
            'batch_dir': str(batch_dir),
            'metadata': batch_meta,
            'artifacts_included': list(available_artifacts.keys()),
            'artifacts_missing': missing_artifacts
        }
    
    def update_batch_index(self, am_batch_info=None, eod_batch_info=None):
        """Update or create BATCH_INDEX.md"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        index_dir = self.audit_base / timestamp
        index_dir.mkdir(parents=True, exist_ok=True)
        
        index_file = index_dir / 'BATCH_INDEX.md'
        
        current_sha = os.getenv('CI_COMMIT_SHORT_SHA', 'local')
        current_time = datetime.now().strftime('%H:%M:%S UTC')
        
        content = f"""# Daily Batch Index

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Date**: {datetime.now().strftime('%Y-%m-%d')} 
**Stability Mode**: {'ON' if self.stability_mode else 'OFF'}

## Batch Status

"""
        
        # AM Batch Status
        if am_batch_info:
            am_status = "✅ READY"
            am_sha = am_batch_info['metadata']['sha']
            am_time = am_batch_info['metadata']['generated'].split()[1]
            am_artifacts = f"{am_batch_info['metadata']['artifacts_included']}/{len(self.batch_artifacts)}"
        else:
            am_status = "⏳ PENDING"
            am_sha = current_sha
            am_time = current_time
            am_artifacts = "0/12"
        
        content += f"**AM**: {am_sha}@{am_time} | Status: {am_status} | Artifacts: {am_artifacts}\n"
        
        # EOD Batch Status  
        if eod_batch_info:
            eod_status = "✅ READY"
            eod_sha = eod_batch_info['metadata']['sha']
            eod_time = eod_batch_info['metadata']['generated'].split()[1]
            eod_artifacts = f"{eod_batch_info['metadata']['artifacts_included']}/{len(self.batch_artifacts)}"
        else:
            eod_status = "⏳ PENDING"
            eod_sha = current_sha
            eod_time = current_time
            eod_artifacts = "0/12"
        
        content += f"**EOD**: {eod_sha}@{eod_time} | Status: {eod_status} | Artifacts: {eod_artifacts}\n"
        
        content += f"""

## Schedule (ET)

### AM Batch
- **Preview**: {self.am_preview_time} ET
- **Email Send**: {self.am_send_time} ET (or {self.am_send_time_macro} ET if Macro Gate)
- **Dashboard Source**: AM batch artifacts
- **Status**: {am_status}

### EOD Batch  
- **Preview**: {self.pm_preview_time} ET
- **Email Send**: {self.pm_send_time} ET
- **EOD Drop**: {self.eod_drop_time} ET
- **Dashboard Source**: EOD batch artifacts
- **Status**: {eod_status}

## Dashboard Integration

Current active batch determines dashboard artifact source:
- **Before {self.eod_drop_time} ET**: Previous EOD batch (if available)
- **After {self.am_send_time} ET**: Current AM batch
- **After {self.eod_drop_time} ET**: Current EOD batch

Missing batch artifacts show "Awaiting batch" badges.

## Stability Mode Configuration

- **STABILITY_MODE**: {self.stability_mode}
- **BATCH_AM_ENABLED**: {self.batch_am_enabled}
- **BATCH_PM_ENABLED**: {self.batch_pm_enabled}
- **CI_GUARD**: {'ACTIVE (blocks non-HOTFIX)' if self.stability_mode else 'INACTIVE'}

---
**BATCH INDEX**: {'Twin batches configured' if self.batch_am_enabled and self.batch_pm_enabled else 'Single batch mode'}
Generated by Stability Batch System v1.0
"""
        
        with open(index_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(index_file)
    
    def run_daily_batch_cycle(self):
        """Run complete daily batch cycle"""
        if not self.stability_mode:
            return {
                'success': False,
                'reason': 'Stability mode not enabled'
            }
        
        # Find latest artifacts
        artifact_dir = self.find_latest_artifacts()
        if not artifact_dir:
            return {
                'success': False,
                'reason': 'No artifact directory found'
            }
        
        # Collect available artifacts
        available_artifacts, missing_artifacts = self.collect_batch_artifacts(artifact_dir)
        
        results = {}
        
        # Create AM batch if enabled
        if self.batch_am_enabled:
            am_batch = self.create_batch_drop('AM', available_artifacts, missing_artifacts)
            results['am_batch'] = am_batch
        
        # Create EOD batch if enabled  
        if self.batch_pm_enabled:
            eod_batch = self.create_batch_drop('EOD', available_artifacts, missing_artifacts)
            results['eod_batch'] = eod_batch
        
        # Update batch index
        batch_index = self.update_batch_index(
            results.get('am_batch'),
            results.get('eod_batch')
        )
        results['batch_index'] = batch_index
        
        return {
            'success': True,
            'results': results,
            'artifacts_found': len(available_artifacts),
            'artifacts_missing': len(missing_artifacts)
        }


def main():
    """Test Stability Batch System"""
    
    # Set stability mode environment
    os.environ['STABILITY_MODE'] = 'true'
    os.environ['BATCH_AM_ENABLED'] = 'true'
    os.environ['BATCH_PM_ENABLED'] = 'true'
    
    batch_system = StabilityBatchSystem()
    
    # Check configuration
    config = batch_system.check_stability_mode()
    print("Stability Mode Configuration:")
    for key, value in config.items():
        print(f"  {key}: {value}")
    
    # Run batch cycle
    print("\nRunning daily batch cycle...")
    result = batch_system.run_daily_batch_cycle()
    
    if result['success']:
        print(f"Batch cycle completed:")
        print(f"  Artifacts found: {result['artifacts_found']}")
        print(f"  Artifacts missing: {result['artifacts_missing']}")
        
        if 'am_batch' in result['results']:
            print(f"  AM Batch: {result['results']['am_batch']['drop_file']}")
            print(f"  AM Zip: {result['results']['am_batch']['zip_file']}")
        
        if 'eod_batch' in result['results']:
            print(f"  EOD Batch: {result['results']['eod_batch']['drop_file']}")
            print(f"  EOD Zip: {result['results']['eod_batch']['zip_file']}")
        
        print(f"  Batch Index: {result['results']['batch_index']}")
    else:
        print(f"Batch cycle failed: {result['reason']}")
    
    return result


if __name__ == '__main__':
    main()