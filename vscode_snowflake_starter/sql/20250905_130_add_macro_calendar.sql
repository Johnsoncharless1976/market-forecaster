-- Add macro calendar for AM kneeboard timing (9:00 vs 9:15)
-- High-impact macro events at 8:30 AM ET delay send to 9:15 AM

CREATE TABLE IF NOT EXISTS MACRO_CALENDAR (
    DATE DATE PRIMARY KEY COMMENT 'Market date (YYYY-MM-DD)',
    EVENT_TIME TIME COMMENT 'Event time (HH:MM:SS format)',
    EVENT_NAME STRING NOT NULL COMMENT 'Macro event name',
    IMPACT_LEVEL STRING NOT NULL COMMENT 'HIGH, MEDIUM, LOW',
    DESCRIPTION STRING COMMENT 'Event description',
    SOURCE STRING DEFAULT 'manual' COMMENT 'Data source: manual, fed, bls, etc',
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Macro calendar for kneeboard timing decisions';

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS IDX_MACRO_CALENDAR_DATE ON MACRO_CALENDAR(DATE, IMPACT_LEVEL);
CREATE INDEX IF NOT EXISTS IDX_MACRO_CALENDAR_IMPACT ON MACRO_CALENDAR(IMPACT_LEVEL, EVENT_TIME);

-- Insert common 8:30 AM ET macro events
INSERT INTO MACRO_CALENDAR (DATE, EVENT_TIME, EVENT_NAME, IMPACT_LEVEL, DESCRIPTION) VALUES
-- September 2025 key events
('2025-09-06', '08:30:00', 'Nonfarm Payrolls', 'HIGH', 'Employment Situation Report - Jobs added, unemployment rate'),
('2025-09-10', '08:30:00', 'Core CPI', 'HIGH', 'Consumer Price Index Core m/m'),
('2025-09-10', '08:30:00', 'CPI', 'HIGH', 'Consumer Price Index m/m'),
('2025-09-12', '08:30:00', 'Core PPI', 'MEDIUM', 'Producer Price Index Core m/m'),
('2025-09-12', '08:30:00', 'PPI', 'MEDIUM', 'Producer Price Index m/m'),
('2025-09-17', '08:30:00', 'Retail Sales', 'HIGH', 'Retail Sales m/m'),
('2025-09-17', '08:30:00', 'Core Retail Sales', 'HIGH', 'Retail Sales ex Auto m/m'),
('2025-09-26', '08:30:00', 'Jobless Claims', 'MEDIUM', 'Initial Jobless Claims'),

-- October 2025 key events  
('2025-10-01', '08:30:00', 'Core PCE Price Index', 'HIGH', 'Personal Consumption Expenditures Price Index Core m/m'),
('2025-10-01', '08:30:00', 'Personal Income', 'MEDIUM', 'Personal Income m/m'),
('2025-10-03', '08:30:00', 'Nonfarm Payrolls', 'HIGH', 'Employment Situation Report - Jobs added, unemployment rate'),
('2025-10-10', '08:30:00', 'Core CPI', 'HIGH', 'Consumer Price Index Core m/m'),
('2025-10-10', '08:30:00', 'CPI', 'HIGH', 'Consumer Price Index m/m'),
('2025-10-15', '08:30:00', 'Retail Sales', 'HIGH', 'Retail Sales m/m'),
('2025-10-15', '08:30:00', 'Core Retail Sales', 'HIGH', 'Retail Sales ex Auto m/m'),
('2025-10-24', '08:30:00', 'Jobless Claims', 'MEDIUM', 'Initial Jobless Claims'),
('2025-10-31', '08:30:00', 'Core PCE Price Index', 'HIGH', 'Personal Consumption Expenditures Price Index Core m/m'),

-- November 2025 key events
('2025-11-07', '08:30:00', 'Nonfarm Payrolls', 'HIGH', 'Employment Situation Report - Jobs added, unemployment rate'),
('2025-11-13', '08:30:00', 'Core CPI', 'HIGH', 'Consumer Price Index Core m/m'),
('2025-11-13', '08:30:00', 'CPI', 'HIGH', 'Consumer Price Index m/m'),
('2025-11-19', '08:30:00', 'Retail Sales', 'HIGH', 'Retail Sales m/m'),
('2025-11-19', '08:30:00', 'Core Retail Sales', 'HIGH', 'Retail Sales ex Auto m/m'),
('2025-11-21', '08:30:00', 'Jobless Claims', 'MEDIUM', 'Initial Jobless Claims'),

-- December 2025 key events
('2025-12-05', '08:30:00', 'Nonfarm Payrolls', 'HIGH', 'Employment Situation Report - Jobs added, unemployment rate'),
('2025-12-11', '08:30:00', 'Core CPI', 'HIGH', 'Consumer Price Index Core m/m'),
('2025-12-11', '08:30:00', 'CPI', 'HIGH', 'Consumer Price Index m/m'),
('2025-12-17', '08:30:00', 'Retail Sales', 'HIGH', 'Retail Sales m/m'),
('2025-12-17', '08:30:00', 'Core Retail Sales', 'HIGH', 'Retail Sales ex Auto m/m'),
('2025-12-19', '08:30:00', 'Jobless Claims', 'MEDIUM', 'Initial Jobless Claims')

ON CONFLICT (DATE, EVENT_TIME, EVENT_NAME) DO NOTHING;

-- Create function to check if date has high-impact macro at 8:30 AM
CREATE OR REPLACE FUNCTION HAS_HIGH_IMPACT_MACRO(check_date DATE)
RETURNS BOOLEAN
LANGUAGE SQL
AS $$
    SELECT COUNT(*) > 0
    FROM MACRO_CALENDAR 
    WHERE DATE = check_date 
      AND EVENT_TIME = '08:30:00'
      AND IMPACT_LEVEL = 'HIGH'
$$;

-- Verify the data
SELECT 'Macro calendar high-impact 8:30 AM events:' as info;
SELECT DATE, EVENT_NAME, IMPACT_LEVEL, DESCRIPTION 
FROM MACRO_CALENDAR 
WHERE EVENT_TIME = '08:30:00' AND IMPACT_LEVEL = 'HIGH'
ORDER BY DATE;

SELECT 'Function test for 2025-09-06 (NFP day):' as info;
SELECT HAS_HIGH_IMPACT_MACRO('2025-09-06') as has_macro;

SELECT 'Function test for 2025-09-05 (normal day):' as info;
SELECT HAS_HIGH_IMPACT_MACRO('2025-09-05') as has_macro;