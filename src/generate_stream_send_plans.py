"""
Generate AM/PM Send Plan CSVs showing per-stream consent filtering
Demonstrates hard separation of AM and PM kneeboards with independent consent
"""

import os
import csv
from datetime import datetime
from typing import List, Dict, Tuple
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

class StreamSendPlanGenerator:
    """Generate send plans for AM and PM streams independently"""
    
    def __init__(self):
        """Initialize send plan generator"""
        self.conn_params = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA')
        }
    
    def get_db_connection(self):
        """Get Snowflake database connection"""
        return snowflake.connector.connect(**self.conn_params)
    
    def get_recipient_stream_status(self) -> List[Dict]:
        """Get all recipients with their stream consent status"""
        
        try:
            with self.get_db_connection() as conn:
                cur = conn.cursor()
                
                cur.execute("""
                    SELECT 
                        EMAIL,
                        COHORT,
                        CONSENT_TS IS NOT NULL as has_general_consent,
                        UNSUBSCRIBED_AT IS NOT NULL as is_globally_unsubscribed,
                        COALESCE(AM_CONSENT, FALSE) as am_consent,
                        COALESCE(PM_CONSENT, FALSE) as pm_consent,
                        AM_UNSUBSCRIBED_AT IS NOT NULL as am_unsubscribed,
                        PM_UNSUBSCRIBED_AT IS NOT NULL as pm_unsubscribed,
                        CASE WHEN CONSENT_TS IS NOT NULL 
                             THEN DATEDIFF(day, CONSENT_TS, CURRENT_TIMESTAMP()) 
                             ELSE NULL END as consent_age_days,
                        CREATED_AT
                    FROM EMAIL_RECIPIENTS
                    ORDER BY EMAIL
                """)
                
                recipients = []
                for row in cur.fetchall():
                    (email, cohort, has_general_consent, is_globally_unsubscribed, 
                     am_consent, pm_consent, am_unsubscribed, pm_unsubscribed, 
                     consent_age_days, created_at) = row
                    
                    recipients.append({
                        'email': email,
                        'cohort': cohort or 'unknown',
                        'has_general_consent': has_general_consent,
                        'is_globally_unsubscribed': is_globally_unsubscribed,
                        'am_consent': am_consent,
                        'pm_consent': pm_consent,
                        'am_unsubscribed': am_unsubscribed,
                        'pm_unsubscribed': pm_unsubscribed,
                        'consent_age_days': consent_age_days or 0,
                        'created_at': created_at
                    })
                
                return recipients
                
        except Exception as e:
            print(f"⚠️ Error getting recipient stream status: {e}")
            return []
    
    def determine_stream_eligibility(self, recipient: Dict, stream_type: str) -> Tuple[bool, str]:
        """Determine if recipient is eligible for specific stream"""
        
        # Check global constraints first
        if not recipient['has_general_consent']:
            return False, "no_general_consent"
        
        if recipient['is_globally_unsubscribed']:
            return False, "globally_unsubscribed"
        
        # Check stream-specific constraints
        if stream_type == 'AM':
            if not recipient['am_consent']:
                return False, "no_am_consent"
            if recipient['am_unsubscribed']:
                return False, "am_unsubscribed"
        elif stream_type == 'PM':
            if not recipient['pm_consent']:
                return False, "no_pm_consent"
            if recipient['pm_unsubscribed']:
                return False, "pm_unsubscribed"
        else:
            return False, "unknown_stream_type"
        
        # All checks passed
        return True, "eligible"
    
    def generate_am_send_plan(self, timestamp: str) -> str:
        """Generate AM stream send plan CSV"""
        
        recipients = self.get_recipient_stream_status()
        
        # Create output directory
        os.makedirs("audit_exports/stream_plans", exist_ok=True)
        
        # Generate AM send plan
        am_plan_path = f"audit_exports/stream_plans/{timestamp}_AM_send_plan.csv"
        
        with open(am_plan_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Headers specific to AM stream
            writer.writerow([
                "email_masked", "cohort", "general_consent", "consent_age_days",
                "am_consent", "am_unsubscribed", "send_status", "reason",
                "macro_gate_applies", "send_time", "preview_time"
            ])
            
            eligible_count = 0
            for recipient in recipients:
                # Mask email for privacy
                masked_email = f"{recipient['email'][:3]}***@{recipient['email'].split('@')[1]}"
                
                # Check AM eligibility
                is_eligible, reason = self.determine_stream_eligibility(recipient, 'AM')
                send_status = "ELIGIBLE" if is_eligible else "SKIP"
                
                if is_eligible:
                    eligible_count += 1
                
                # AM stream always has macro gate logic
                writer.writerow([
                    masked_email,
                    recipient['cohort'],
                    recipient['has_general_consent'],
                    recipient['consent_age_days'],
                    recipient['am_consent'],
                    recipient['am_unsubscribed'],
                    send_status,
                    reason,
                    "TRUE",  # AM always subject to macro gate
                    "9:00_or_9:15_ET",
                    "8:40_or_8:55_ET"
                ])
        
        print(f"Generated AM send plan: {am_plan_path}")
        print(f"   Total recipients: {len(recipients)}, AM eligible: {eligible_count}")
        
        return am_plan_path
    
    def generate_pm_send_plan(self, timestamp: str) -> str:
        """Generate PM stream send plan CSV"""
        
        recipients = self.get_recipient_stream_status()
        
        # Create output directory
        os.makedirs("audit_exports/stream_plans", exist_ok=True)
        
        # Generate PM send plan
        pm_plan_path = f"audit_exports/stream_plans/{timestamp}_PM_send_plan.csv"
        
        with open(pm_plan_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Headers specific to PM stream
            writer.writerow([
                "email_masked", "cohort", "general_consent", "consent_age_days",
                "pm_consent", "pm_unsubscribed", "send_status", "reason",
                "postmortem_gate", "send_time", "preview_time"
            ])
            
            eligible_count = 0
            for recipient in recipients:
                # Mask email for privacy
                masked_email = f"{recipient['email'][:3]}***@{recipient['email'].split('@')[1]}"
                
                # Check PM eligibility
                is_eligible, reason = self.determine_stream_eligibility(recipient, 'PM')
                send_status = "ELIGIBLE" if is_eligible else "SKIP"
                
                if is_eligible:
                    eligible_count += 1
                
                # PM stream has post-mortem gate logic
                writer.writerow([
                    masked_email,
                    recipient['cohort'],
                    recipient['has_general_consent'],
                    recipient['consent_age_days'],
                    recipient['pm_consent'],
                    recipient['pm_unsubscribed'],
                    send_status,
                    reason,
                    "REQUIRED",  # PM requires post-mortem completion
                    "17:00_ET",
                    "16:45_ET"
                ])
        
        print(f"Generated PM send plan: {pm_plan_path}")
        print(f"   Total recipients: {len(recipients)}, PM eligible: {eligible_count}")
        
        return pm_plan_path
    
    def generate_stream_summary(self, timestamp: str) -> str:
        """Generate summary report comparing AM/PM streams"""
        
        recipients = self.get_recipient_stream_status()
        
        # Calculate stream metrics
        total_recipients = len(recipients)
        am_eligible = sum(1 for r in recipients if self.determine_stream_eligibility(r, 'AM')[0])
        pm_eligible = sum(1 for r in recipients if self.determine_stream_eligibility(r, 'PM')[0])
        both_streams = sum(1 for r in recipients 
                          if self.determine_stream_eligibility(r, 'AM')[0] 
                          and self.determine_stream_eligibility(r, 'PM')[0])
        am_only = sum(1 for r in recipients 
                     if self.determine_stream_eligibility(r, 'AM')[0] 
                     and not self.determine_stream_eligibility(r, 'PM')[0])
        pm_only = sum(1 for r in recipients 
                     if not self.determine_stream_eligibility(r, 'AM')[0] 
                     and self.determine_stream_eligibility(r, 'PM')[0])
        no_streams = sum(1 for r in recipients 
                        if not self.determine_stream_eligibility(r, 'AM')[0] 
                        and not self.determine_stream_eligibility(r, 'PM')[0])
        
        # Generate summary report
        summary_path = f"audit_exports/stream_plans/{timestamp}_stream_summary.md"
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(f"""# AM/PM Stream Send Plan Summary

*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*

## Stream Separation Overview

This report demonstrates the hard separation of AM and PM kneeboard streams with independent consent tracking.

## Recipient Distribution

| Metric | Count | Percentage |
|--------|-------|------------|
| Total Recipients | {total_recipients} | 100.0% |
| AM Eligible | {am_eligible} | {(am_eligible/total_recipients*100):.1f}% |
| PM Eligible | {pm_eligible} | {(pm_eligible/total_recipients*100):.1f}% |
| Both Streams | {both_streams} | {(both_streams/total_recipients*100):.1f}% |
| AM Only | {am_only} | {(am_only/total_recipients*100):.1f}% |
| PM Only | {pm_only} | {(pm_only/total_recipients*100):.1f}% |
| No Streams | {no_streams} | {(no_streams/total_recipients*100):.1f}% |

## Stream Characteristics

### AM Stream (Morning Kneeboard)
- **Send Time**: 9:00 AM ET (normal) or 9:15 AM ET (macro gate)
- **Preview Time**: 8:40 AM ET (normal) or 8:55 AM ET (macro gate)  
- **Content**: Market outlook, key levels, technical context
- **Gate Logic**: High-impact 8:30 AM macro events delay send by 15 minutes
- **Consent Field**: `AM_CONSENT` in EMAIL_RECIPIENTS table
- **Unsubscribe Field**: `AM_UNSUBSCRIBED_AT`

### PM Stream (Evening Kneeboard)  
- **Send Time**: 5:00 PM ET (fixed)
- **Preview Time**: 4:45 PM ET (fixed)
- **Content**: Post-mortem analysis, performance metrics, next day preview
- **Gate Logic**: Requires post-mortem scoring and miss tags completion
- **Consent Field**: `PM_CONSENT` in EMAIL_RECIPIENTS table
- **Unsubscribe Field**: `PM_UNSUBSCRIBED_AT`

## Template Separation

AM and PM kneeboards use **completely separate templates** with no shared content:

- **AM Template**: Forward-looking market analysis and trading setup
- **PM Template**: Backward-looking performance review and lessons learned
- **Independent RUN_IDs**: `AM_YYYYMMDD_HHMMSS` vs `PM_YYYYMMDD_HHMMSS`
- **Separate Send Logs**: All sends logged with distinct badges and metadata
- **Independent Consent**: Users can subscribe/unsubscribe from each stream separately

## Evidence Files

- `{timestamp}_AM_send_plan.csv` - AM stream eligibility and timing
- `{timestamp}_PM_send_plan.csv` - PM stream eligibility and timing  
- `{timestamp}_stream_summary.md` - This summary report

## Compliance Notes

- Stream preferences are logged in `STREAM_PREFERENCE_LOG` for audit trail
- Global unsubscribe (`UNSUBSCRIBED_AT`) still applies to both streams
- General consent (`CONSENT_TS`) is required for any email delivery
- Each stream respects its own consent and unsubscribe fields independently
""")
        
        print(f"Generated stream summary: {summary_path}")
        
        return summary_path
    
    def generate_all_plans(self) -> Dict[str, str]:
        """Generate all stream send plans and summary"""
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        print(f"Generating AM/PM stream send plans - {timestamp}")
        
        # Generate individual plans
        am_plan = self.generate_am_send_plan(timestamp)
        pm_plan = self.generate_pm_send_plan(timestamp)
        summary = self.generate_stream_summary(timestamp)
        
        return {
            'am_plan': am_plan,
            'pm_plan': pm_plan,
            'summary': summary,
            'timestamp': timestamp
        }


def main():
    """Main function to generate stream send plans"""
    
    generator = StreamSendPlanGenerator()
    results = generator.generate_all_plans()
    
    print(f"\nStream Send Plan Generation Complete:")
    print(f"   AM Plan: {results['am_plan']}")
    print(f"   PM Plan: {results['pm_plan']}")
    print(f"   Summary: {results['summary']}")
    print(f"   Timestamp: {results['timestamp']}")


if __name__ == "__main__":
    main()