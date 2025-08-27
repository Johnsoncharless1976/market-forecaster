-- Add AM/PM stream separation and per-stream consent tracking
-- Enables independent subscription management for morning vs evening kneeboards

-- Extend EMAIL_RECIPIENTS with stream-specific consent
ALTER TABLE EMAIL_RECIPIENTS 
ADD COLUMN IF NOT EXISTS AM_CONSENT BOOLEAN DEFAULT TRUE COMMENT 'Consent for AM kneeboard (morning stream)';

ALTER TABLE EMAIL_RECIPIENTS 
ADD COLUMN IF NOT EXISTS PM_CONSENT BOOLEAN DEFAULT TRUE COMMENT 'Consent for PM kneeboard (evening stream)';

ALTER TABLE EMAIL_RECIPIENTS 
ADD COLUMN IF NOT EXISTS AM_UNSUBSCRIBED_AT TIMESTAMP_LTZ COMMENT 'When user unsubscribed from AM stream';

ALTER TABLE EMAIL_RECIPIENTS 
ADD COLUMN IF NOT EXISTS PM_UNSUBSCRIBED_AT TIMESTAMP_LTZ COMMENT 'When user unsubscribed from PM stream';

-- Create stream preference log for audit trail
CREATE TABLE IF NOT EXISTS STREAM_PREFERENCE_LOG (
    LOG_ID STRING PRIMARY KEY COMMENT 'Unique log entry ID',
    EMAIL STRING NOT NULL COMMENT 'Recipient email address',
    STREAM_TYPE STRING NOT NULL COMMENT 'AM or PM',
    ACTION STRING NOT NULL COMMENT 'SUBSCRIBE, UNSUBSCRIBE, PREFERENCE_CHANGE',
    OLD_VALUE BOOLEAN COMMENT 'Previous consent value',
    NEW_VALUE BOOLEAN COMMENT 'New consent value',
    CHANGE_SOURCE STRING COMMENT 'How change was made: email_link, web_portal, admin, etc',
    IP_ADDRESS STRING COMMENT 'Source IP for anti-abuse',
    USER_AGENT STRING COMMENT 'Browser user agent',
    CHANGE_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (EMAIL) REFERENCES EMAIL_RECIPIENTS(EMAIL)
) COMMENT = 'Audit log for AM/PM stream preference changes';

-- Create indexes for efficient stream preference queries
CREATE INDEX IF NOT EXISTS IDX_EMAIL_RECIPIENTS_AM_CONSENT 
ON EMAIL_RECIPIENTS(AM_CONSENT, AM_UNSUBSCRIBED_AT);

CREATE INDEX IF NOT EXISTS IDX_EMAIL_RECIPIENTS_PM_CONSENT 
ON EMAIL_RECIPIENTS(PM_CONSENT, PM_UNSUBSCRIBED_AT);

CREATE INDEX IF NOT EXISTS IDX_STREAM_PREFERENCE_LOG_EMAIL 
ON STREAM_PREFERENCE_LOG(EMAIL, CHANGE_AT);

CREATE INDEX IF NOT EXISTS IDX_STREAM_PREFERENCE_LOG_STREAM 
ON STREAM_PREFERENCE_LOG(STREAM_TYPE, CHANGE_AT);

-- Update existing recipients to have both AM and PM consent by default
UPDATE EMAIL_RECIPIENTS 
SET AM_CONSENT = TRUE, PM_CONSENT = TRUE, UPDATED_AT = CURRENT_TIMESTAMP()
WHERE AM_CONSENT IS NULL OR PM_CONSENT IS NULL;

-- Create function to check AM eligibility
CREATE OR REPLACE FUNCTION IS_AM_ELIGIBLE(recipient_email STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS $$
    SELECT COALESCE(AM_CONSENT, FALSE) = TRUE 
           AND AM_UNSUBSCRIBED_AT IS NULL 
           AND UNSUBSCRIBED_AT IS NULL  -- Global unsubscribe still applies
           AND CONSENT_TS IS NOT NULL   -- Must have general consent
    FROM EMAIL_RECIPIENTS 
    WHERE EMAIL = recipient_email
$$;

-- Create function to check PM eligibility  
CREATE OR REPLACE FUNCTION IS_PM_ELIGIBLE(recipient_email STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS $$
    SELECT COALESCE(PM_CONSENT, FALSE) = TRUE 
           AND PM_UNSUBSCRIBED_AT IS NULL 
           AND UNSUBSCRIBED_AT IS NULL  -- Global unsubscribe still applies
           AND CONSENT_TS IS NOT NULL   -- Must have general consent
    FROM EMAIL_RECIPIENTS 
    WHERE EMAIL = recipient_email
$$;

-- Insert sample preference log entries
INSERT INTO STREAM_PREFERENCE_LOG (
    LOG_ID, EMAIL, STREAM_TYPE, ACTION, OLD_VALUE, NEW_VALUE, 
    CHANGE_SOURCE, CHANGE_AT
) VALUES
('INIT_AM_001', 'trader1@zenmarket.ai', 'AM', 'SUBSCRIBE', NULL, TRUE, 'migration', CURRENT_TIMESTAMP()),
('INIT_PM_001', 'trader1@zenmarket.ai', 'PM', 'SUBSCRIBE', NULL, TRUE, 'migration', CURRENT_TIMESTAMP()),
('INIT_AM_002', 'analyst@zenmarket.ai', 'AM', 'SUBSCRIBE', NULL, TRUE, 'migration', CURRENT_TIMESTAMP()),
('INIT_PM_002', 'analyst@zenmarket.ai', 'PM', 'SUBSCRIBE', NULL, TRUE, 'migration', CURRENT_TIMESTAMP())
ON CONFLICT (LOG_ID) DO NOTHING;

-- Verify the enhanced structure
SELECT 'EMAIL_RECIPIENTS with stream consent:' as info;
DESCRIBE TABLE EMAIL_RECIPIENTS;

SELECT 'STREAM_PREFERENCE_LOG structure:' as info;
DESCRIBE TABLE STREAM_PREFERENCE_LOG;

-- Test the eligibility functions
SELECT 'AM eligibility test:' as info;
SELECT EMAIL, IS_AM_ELIGIBLE(EMAIL) as am_eligible
FROM EMAIL_RECIPIENTS 
WHERE EMAIL IN ('trader1@zenmarket.ai', 'analyst@zenmarket.ai');

SELECT 'PM eligibility test:' as info;
SELECT EMAIL, IS_PM_ELIGIBLE(EMAIL) as pm_eligible
FROM EMAIL_RECIPIENTS 
WHERE EMAIL IN ('trader1@zenmarket.ai', 'analyst@zenmarket.ai');

-- Show stream preference summary
SELECT 'Stream consent summary:' as info;
SELECT 
    COUNT(*) as total_users,
    COUNT(CASE WHEN AM_CONSENT = TRUE THEN 1 END) as am_subscribers,
    COUNT(CASE WHEN PM_CONSENT = TRUE THEN 1 END) as pm_subscribers,
    COUNT(CASE WHEN AM_CONSENT = TRUE AND PM_CONSENT = TRUE THEN 1 END) as both_streams,
    COUNT(CASE WHEN AM_CONSENT = FALSE AND PM_CONSENT = FALSE THEN 1 END) as no_streams
FROM EMAIL_RECIPIENTS;