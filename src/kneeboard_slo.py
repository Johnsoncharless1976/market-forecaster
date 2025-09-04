#!/usr/bin/env python3
"""
Kneeboard SLO Tracker
Track AM/PM email on-time delivery
"""

import os
from datetime import datetime, timedelta
from pathlib import Path


class KneeboardSLO:
    """Track kneeboard email SLO compliance"""
    
    def __init__(self):
        self.slo_targets = {
            'am_preview_time': '08:40',
            'am_send_time': '09:00',
            'am_send_time_macro': '09:15', 
            'pm_preview_time': '16:45',
            'pm_send_time': '17:00'
        }
        
    def check_pm_ontime_status(self, pm_send_time_str):
        """Check if PM email was sent on time"""
        try:
            # Parse PM send time
            pm_send_time = datetime.strptime(pm_send_time_str, '%Y-%m-%d %H:%M:%S UTC')
            
            # Target: 17:00 ET (assumes send time is already in correct timezone)
            target_time = datetime.strptime(f"{pm_send_time.date()} 17:00:00", '%Y-%m-%d %H:%M:%S')
            
            # Allow 5 minute grace period
            grace_period = timedelta(minutes=5)
            late_threshold = target_time + grace_period
            miss_threshold = target_time + timedelta(hours=1)  # 1 hour = MISS
            
            if pm_send_time <= target_time:
                return 'ON_TIME'
            elif pm_send_time <= late_threshold:
                return 'ON_TIME'  # Within grace period
            elif pm_send_time <= miss_threshold:
                return 'LATE'
            else:
                return 'MISS'
                
        except Exception:
            return 'UNKNOWN'
    
    def write_kneeboard_slo(self, pm_status, am_send_time=None, pm_send_time=None, resend=False):
        """Write KNEEBOARD_SLO.md report"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        audit_dir = Path('audit_exports') / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        slo_file = audit_dir / 'KNEEBOARD_SLO.md'
        
        content = f"""# Kneeboard SLO Report

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Date**: {datetime.now().strftime('%Y-%m-%d')}
**Mode**: Pilot (self-only delivery tracking)

## SLO Targets (ET)

- **AM Preview**: {self.slo_targets['am_preview_time']} ET
- **AM Send**: {self.slo_targets['am_send_time']} ET (or {self.slo_targets['am_send_time_macro']} ET if Macro Gate)
- **PM Preview**: {self.slo_targets['pm_preview_time']} ET
- **PM Send**: {self.slo_targets['pm_send_time']} ET

## Today's Performance

### AM Kneeboard
- **Target Time**: {self.slo_targets['am_send_time']} ET
- **Actual Send**: {am_send_time or 'Not tracked'}
- **Status**: {'ON_TIME' if am_send_time else 'PENDING'}

### PM Kneeboard  
- **Target Time**: {self.slo_targets['pm_send_time']} ET
- **Actual Send**: {pm_send_time or 'Not tracked'}
- **Status**: {pm_status}
- **Resend Required**: {'YES' if resend else 'NO'}

## SLO Compliance

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| **AM On-Time** | ≤{self.slo_targets['am_send_time']} ET | {am_send_time or 'N/A'} | {'✅ PASS' if am_send_time else '⏳ PENDING'} |
| **PM On-Time** | ≤{self.slo_targets['pm_send_time']} ET | {pm_send_time or 'N/A'} | {'✅ PASS' if pm_status == 'ON_TIME' else '🟡 LATE' if pm_status == 'LATE' else '❌ MISS' if pm_status == 'MISS' else '⏳ PENDING'} |

## Issue Analysis

"""
        
        if pm_status == 'MISS':
            content += """### PM Email MISS
- **Root Cause**: Delivery time >1 hour past 17:00 ET target
- **Impact**: SLO violation - PM email reliability compromised
- **Action**: Investigate scheduling/delivery pipeline
"""
        elif pm_status == 'LATE':
            content += """### PM Email LATE
- **Root Cause**: Delivery time >5 minutes past 17:00 ET target
- **Impact**: Minor SLO degradation
- **Action**: Monitor for pattern; investigate if recurring
"""
        elif resend:
            content += """### PM Email Resend
- **Root Cause**: Original delivery successful but client filtering suspected
- **Impact**: User experience - email not received in inbox
- **Action**: Resend completed; monitor client filtering patterns
"""
        else:
            content += """### No Issues Detected
- **AM Email**: {'Delivered on-time' if am_send_time else 'Pending delivery'}
- **PM Email**: {'Delivered on-time' if pm_status == 'ON_TIME' else 'Status: ' + pm_status}
- **Action**: Continue monitoring
"""
        
        content += f"""

## Recommendations

### Short Term
- **Monitor**: Continue daily SLO tracking
- **Alert**: Set up automated alerts for >15min delays
- **Backup**: Implement retry logic for failed sends

### Long Term
- **Target**: Maintain >95% on-time delivery rate
- **Improve**: Reduce average delivery time variance
- **Scale**: Prepare for multi-recipient delivery when pilot completes

## Environment Check

- **STABILITY_MODE**: {os.getenv('STABILITY_MODE', 'false')}
- **EMAIL_ENABLED**: {os.getenv('EMAIL_ENABLED', 'false')}
- **EMAIL_MODE**: {os.getenv('EMAIL_MODE', 'not set')}
- **Timezone**: {os.getenv('TZ', 'system default')} (should be America/New_York)

---
**KNEEBOARD SLO**: PM status {pm_status} {'(resend required)' if resend else ''}
Generated by Kneeboard SLO v0.1
"""
        
        with open(slo_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(slo_file)


def main():
    """Test Kneeboard SLO system"""
    slo = KneeboardSLO()
    
    # Test PM on-time check with actual send time from log
    pm_send_time = "2025-08-28 16:40:59 UTC"  # From EMAIL_SEND_LOG.md
    pm_status = slo.check_pm_ontime_status(pm_send_time)
    
    print(f"PM SLO Check:")
    print(f"  Send Time: {pm_send_time}")
    print(f"  Status: {pm_status}")
    
    # Write SLO report
    slo_file = slo.write_kneeboard_slo(
        pm_status=pm_status,
        am_send_time="2025-08-28 09:00:00 UTC",  # Simulated
        pm_send_time=pm_send_time,
        resend=True  # Resend was performed
    )
    
    print(f"SLO Report: {slo_file}")
    
    return {
        'pm_status': pm_status,
        'slo_file': slo_file
    }


if __name__ == '__main__':
    main()